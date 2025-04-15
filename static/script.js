let extraExact = [];
let extraPartial = [];

function formatResultEntry(entry) {
    const url = entry[0];
    const score = parseFloat(entry[1]).toFixed(4);
    return `<p class="result-entry">
                <a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a>
                <span class="score">(Score: ${score})</span>
            </p>`;
}

function displayResults(data) {
    const resultDiv = document.getElementById("results");
    resultDiv.innerHTML = "";

    if (data.message) {
        resultDiv.innerHTML = `<p>${data.message}</p>`;
        return;
    }

    const exactDiv = document.createElement("div");
    exactDiv.classList.add("result-section");
    exactDiv.innerHTML = "<h3>Exact Matches:</h3>";
    const exactList = document.createElement('div');
    data.exact.forEach(entry => {
        exactList.innerHTML += formatResultEntry(entry);
    });
    exactDiv.appendChild(exactList);

    if (data.more_exact && data.more_exact.length > 0) {
        extraExact = data.more_exact;
        const moreBtn = document.createElement('div');
        moreBtn.classList.add("more-btn");
        moreBtn.textContent = `Show ${extraExact.length} More Exact Matches`;
        moreBtn.onclick = () => showMore('exact', moreBtn);
        exactDiv.appendChild(moreBtn);
    }

    const partialDiv = document.createElement("div");
    partialDiv.classList.add("result-section");
    partialDiv.innerHTML = "<h3>Partial Matches:</h3>";
    const partialList = document.createElement('div');
    data.partial.forEach(entry => {
        partialList.innerHTML += formatResultEntry(entry);
    });
    partialDiv.appendChild(partialList);

    if (data.more_partial && data.more_partial.length > 0) {
        extraPartial = data.more_partial;
        const moreBtn = document.createElement('div');
        moreBtn.classList.add("more-btn");
        moreBtn.textContent = `Show ${extraPartial.length} More Partial Matches`;
        moreBtn.onclick = () => showMore('partial', moreBtn);
        partialDiv.appendChild(moreBtn);
    }

    resultDiv.appendChild(exactDiv);

    if (data.exact.length > 0 && data.partial.length > 0) {
       const hr = document.createElement('hr');
       resultDiv.appendChild(hr);
    }
    resultDiv.appendChild(partialDiv);
}

function showMore(type, buttonElement) {
    const data = (type === "exact") ? extraExact : extraPartial;
    if (!data || data.length === 0) return;

    const parentSection = buttonElement.parentElement;
    const listContainer = parentSection.querySelector('div:not(.more-btn)'); 

    if (listContainer) {
        data.forEach(entry => {
            listContainer.innerHTML += formatResultEntry(entry);
        });
    } else {
        console.error("Could not find list container for", type);
    }

    if (type === "exact") {
        extraExact = [];
    } else {
        extraPartial = [];
    }
    buttonElement.remove();
}

function submitSearch() {
    const query = document.getElementById("searchInput").value.trim();
    if (!query) {
        document.getElementById("results").innerHTML = "<p>Please enter a search query.</p>";
        return;
    }

    document.getElementById("results").innerHTML = "<p>Searching...</p>";

    fetch("/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query })
    })
    .then(response => {
        if (!response.ok) {
            return response.json().then(err => {
                 throw new Error(err.error || `HTTP error! status: ${response.status}`);
            });
        }
        return response.json();
    })
    .then(data => {
        displayResults(data);
    })
    .catch(err => {
        document.getElementById("results").innerHTML = `<p>Error: ${err.message || 'Failed to fetch results'}</p>`;
        console.error("Search Error:", err);
    });
}

document.getElementById('searchInput').addEventListener('keypress', function (e) {
    if (e.key === 'Enter') {
        submitSearch();
    }
});