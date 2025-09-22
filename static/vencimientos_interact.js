document.addEventListener('DOMContentLoaded', function() {
    const kpiCards = document.querySelectorAll('.kpi-card');
    const tableRows = document.querySelectorAll('.vencimientos-table tbody tr');
    const searchInput = document.getElementById('searchInput');

    function filterTable() {
        const searchTerm = searchInput.value.toLowerCase();
        let activeCard = document.querySelector('.kpi-card.active');

        tableRows.forEach(row => {
            const tomador = row.querySelector('[data-label="Tomador"]').textContent.toLowerCase();
            let showRow = tomador.includes(searchTerm);

            if (activeCard) {
                const filterDays = activeCard.dataset.filterDays;
                const filterRamo = activeCard.dataset.filterRamo;

                if (filterDays) {
                    const diasVencer = parseInt(row.dataset.diasVencer, 10);
                    if (diasVencer > parseInt(filterDays, 10)) {
                        showRow = false;
                    }
                }

                if (filterRamo) {
                    const ramo = row.dataset.ramo;
                    if (ramo.toLowerCase().indexOf(filterRamo.toLowerCase()) === -1) {
                        showRow = false;
                    }
                }
            }

            row.style.display = showRow ? '' : 'none';
        });
    }

    if (searchInput) {
        searchInput.addEventListener('input', filterTable);
    }

    kpiCards.forEach(card => {
        card.addEventListener('click', function() {
            if (this.classList.contains('active')) {
                this.classList.remove('active');
            } else {
                kpiCards.forEach(c => c.classList.remove('active'));
                this.classList.add('active');
            }
            filterTable();
        });
    });
});