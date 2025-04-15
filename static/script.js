let extraExact = [];
let extraPartial = [];

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
    data.exact.forEach(entry => {
        exactDiv.innerHTML += `<p class="result-url">URL: ${entry[0]}, Score: ${entry[1]}</p>`;
    });

    if (data.more_exact.length > 0) {
        extraExact = data.more_exact;
        exactDiv.innerHTML += `<div class="more-btn" onclick="showMore('exact')">More</div>`;
    }

    const partialDiv = document.createElement("div");
    partialDiv.classList.add("result-section");
    partialDiv.innerHTML = "<h3>Partial Matches:</h3>";
    data.partial.forEach(entry => {
        partialDiv.innerHTML += `<p class="result-url">URL: ${entry[0]}, Score: ${entry[1]}</p>`;
    });

    if (data.more_partial.length > 0) {
        extraPartial = data.more_partial;
        partialDiv.innerHTML += `<div class="more-btn" onclick="showMore('partial')">More</div>`;
    }

    resultDiv.appendChild(exactDiv);
    resultDiv.appendChild(partialDiv);
}

function showMore(type) {
    const resultDiv = document.getElementById("results");
    const container = document.createElement("div");

    const data = (type === "exact") ? extraExact : extraPartial;
    const title = (type === "exact") ? "Exact Matches (More):" : "Partial Matches (More):";

    container.innerHTML = `<h4>${title}</h4>`;
    data.forEach(entry => {
        container.innerHTML += `<p class="result-url">URL: ${entry[0]}, Score: ${entry[1]}</p>`;
    });

    resultDiv.appendChild(container);
}

function submitSearch() {
    const query = document.getElementById("searchInput").value;
    if (!query) return;

    fetch("/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query })
    })
    .then(res => res.json())
    .then(data => displayResults(data))
    .catch(err => {
        document.getElementById("results").innerHTML = "<p>Error fetching results</p>";
        console.error(err);
    });
}
