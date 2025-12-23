document.addEventListener('DOMContentLoaded', function() {
    // Get references to elements
    const colButtons = document.querySelectorAll('.col-btn');
    const rowButtons = document.querySelectorAll('.row-btn');
    const displayedImage = document.getElementById('displayed-image');
    const noSelectionText = document.getElementById('no-selection');
    const selectionText = document.getElementById('selection-text');
    
    // Track current selection
    let selectedRow = null;
    let selectedCol = null;
    let selectedRowText = '';
    let selectedColText = '';
    
    // Update the displayed image based on selections
    function updateImage() {
        if (selectedRow !== null && selectedCol !== null) {
            // Format the image filename - combine row and column
            const imageFile = `/Visualizations/${selectedRow}${selectedCol}.png`;
            
            // Update the image
            displayedImage.src = imageFile;
            displayedImage.style.display = 'block';
            noSelectionText.style.display = 'none';
            
            // Update selection text with the descriptive names of selected buttons
            selectionText.textContent = `${selectedRowText} by ${selectedColText}`;
            
            console.log(`Loading image: ${imageFile}`);
        } else {
            // No complete selection yet
            displayedImage.style.display = 'none';
            noSelectionText.style.display = 'block';
            selectionText.textContent = 'None';
        }
    }
    
    // Add click event listeners to column buttons
    colButtons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all column buttons
            colButtons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Update selection
            selectedCol = this.getAttribute('data-col');
            selectedColText = this.textContent;
            
            // Update the displayed image
            updateImage();
        });
    });
    
    // Add click event listeners to row buttons
    rowButtons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all row buttons
            rowButtons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Update selection
            selectedRow = this.getAttribute('data-row');
            selectedRowText = this.textContent;
            
            // Update the displayed image
            updateImage();
        });
    });
});