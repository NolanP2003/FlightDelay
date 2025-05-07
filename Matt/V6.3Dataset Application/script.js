document.addEventListener('DOMContentLoaded', function() {
    // ORIGINAL MATRIX VISUALIZATION
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

    // 3D MATRIX VISUALIZATION
    // Get references to 3D matrix elements
    const digit1Buttons = document.querySelectorAll('.digit1-btn');
    const digit2Buttons = document.querySelectorAll('.digit2-btn');
    const digit3Buttons = document.querySelectorAll('.digit3-btn');
    const threedDisplayedImage = document.getElementById('threed-displayed-image');
    const threedNoSelection = document.getElementById('threed-no-selection');
    const threedSelectionText = document.getElementById('threed-selection-text');
    
    // Track current 3D matrix selection
    let selectedDigit1 = null;
    let selectedDigit2 = null;
    let selectedDigit3 = null;
    let selectedDigit1Text = '';
    let selectedDigit2Text = '';
    let selectedDigit3Text = '';
    
    // Update the displayed image based on 3D selections
    function update3DImage() {
        if (selectedDigit1 !== null && selectedDigit2 !== null && selectedDigit3 !== null) {
            // Format the image filename - combine all three digits
            const imageNumber = `${selectedDigit1}${selectedDigit2}${selectedDigit3}`;
            const imageFile = `/Visualizations/${imageNumber}.png`;
            
            // Update the image
            threedDisplayedImage.src = imageFile;
            threedDisplayedImage.style.display = 'block';
            threedNoSelection.style.display = 'none';
            
            // Update selection text with descriptive names
            threedSelectionText.textContent = `${selectedDigit1Text}, ${selectedDigit2Text}, ${selectedDigit3Text}`;
            
            console.log(`Loading 3D matrix image: ${imageFile}`);
        } else {
            // Incomplete selection
            threedDisplayedImage.style.display = 'none';
            threedNoSelection.style.display = 'block';
            threedSelectionText.textContent = 'None';
        }
    }
    
    // Add click event listeners to first digit buttons
    digit1Buttons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all first digit buttons
            digit1Buttons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Update selection
            selectedDigit1 = this.getAttribute('data-digit1');
            selectedDigit1Text = this.textContent;
            
            // Update the displayed image
            update3DImage();
        });
    });
    
    // Add click event listeners to second digit buttons
    digit2Buttons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all second digit buttons
            digit2Buttons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Update selection
            selectedDigit2 = this.getAttribute('data-digit2');
            selectedDigit2Text = this.textContent;
            
            // Update the displayed image
            update3DImage();
        });
    });
    
    // Add click event listeners to third digit buttons
    digit3Buttons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all third digit buttons
            digit3Buttons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Update selection
            selectedDigit3 = this.getAttribute('data-digit3');
            selectedDigit3Text = this.textContent;
            
            // Update the displayed image
            update3DImage();
        });
    });

    // GRID VISUALIZATION
    // Get references to grid elements
    const gridButtons = document.querySelectorAll('.grid-button');
    const gridDisplayedImage = document.getElementById('grid-displayed-image');
    const gridNoSelection = document.getElementById('grid-no-selection');
    const gridSelectionText = document.getElementById('grid-selection-text');
    
    // Add click event listeners to grid buttons
    gridButtons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all grid buttons
            gridButtons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to the clicked button
            this.classList.add('active');
            
            // Get the image number from the data attribute
            const imageNumber = this.getAttribute('data-image');
            
            // Update the displayed image
            gridDisplayedImage.src = `/Visualizations/${imageNumber}.png`;
            gridDisplayedImage.style.display = 'block';
            gridNoSelection.style.display = 'none';
            
            // Update selection text
            gridSelectionText.textContent = this.textContent;
            
            console.log(`Loading image: ${imageNumber}.png`);
        });
    });
});