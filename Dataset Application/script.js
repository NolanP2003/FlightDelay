document.addEventListener('DOMContentLoaded', () => {

    /* ----------------------------------------------------------
       ORIGINAL MATRIX VISUALIZATION
    ----------------------------------------------------------- */

    const colButtons = document.querySelectorAll('.col-btn');
    const rowButtons = document.querySelectorAll('.row-btn');
    const displayedImage = document.getElementById('displayed-image');
    const noSelectionText = document.getElementById('no-selection');
    const selectionText = document.getElementById('selection-text');

    let selectedRow = null;
    let selectedCol = null;
    let selectedRowText = '';
    let selectedColText = '';

    function updateImage() {
        if (selectedRow !== null && selectedCol !== null) {

            const imageFile = `./Visualizations/${selectedRow}${selectedCol}.png`;

            displayedImage.src = imageFile;
            displayedImage.style.display = 'block';
            noSelectionText.style.display = 'none';
            selectionText.textContent = `${selectedRowText} by ${selectedColText}`;

            // handle missing images gracefully
            displayedImage.onerror = () => {
                displayedImage.style.display = 'none';
                noSelectionText.textContent = "Image not found";
                noSelectionText.style.display = 'block';
            };

        } else {
            displayedImage.style.display = 'none';
            noSelectionText.textContent = "Please select row and column buttons";
            noSelectionText.style.display = 'block';
            selectionText.textContent = 'None';
        }
    }

    colButtons.forEach(button => {
        button.addEventListener('click', function () {
            colButtons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            selectedCol = this.getAttribute('data-col');
            selectedColText = this.textContent;

            updateImage();
        });
    });

    rowButtons.forEach(button => {
        button.addEventListener('click', function () {
            rowButtons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            selectedRow = this.getAttribute('data-row');
            selectedRowText = this.textContent;

            updateImage();
        });
    });

    /* ----------------------------------------------------------
       3D MATRIX VISUALIZATION
    ----------------------------------------------------------- */

    const digit1Buttons = document.querySelectorAll('.digit1-btn');
    const digit2Buttons = document.querySelectorAll('.digit2-btn');
    const digit3Buttons = document.querySelectorAll('.digit3-btn');

    const threedDisplayedImage = document.getElementById('threed-displayed-image');
    const threedNoSelection = document.getElementById('threed-no-selection');
    const threedSelectionText = document.getElementById('threed-selection-text');

    let selectedDigit1 = null;
    let selectedDigit2 = null;
    let selectedDigit3 = null;

    let selectedDigit1Text = '';
    let selectedDigit2Text = '';
    let selectedDigit3Text = '';

    function update3DImage() {
        if (selectedDigit1 !== null && selectedDigit2 !== null && selectedDigit3 !== null) {

            const imageNumber = `${selectedDigit1}${selectedDigit2}${selectedDigit3}`;
            const imageFile = `./Visualizations/${imageNumber}.png`;

            threedDisplayedImage.src = imageFile;
            threedDisplayedImage.style.display = 'block';
            threedNoSelection.style.display = 'none';

            threedSelectionText.textContent =
                `${selectedDigit1Text}, ${selectedDigit2Text}, ${selectedDigit3Text}`;

            threedDisplayedImage.onerror = () => {
                threedDisplayedImage.style.display = 'none';
                threedNoSelection.textContent = "Image not found";
                threedNoSelection.style.display = 'block';
            };

        } else {
            threedDisplayedImage.style.display = 'none';
            threedNoSelection.textContent = "Please select buttons from all three dimensions";
            threedNoSelection.style.display = 'block';
            threedSelectionText.textContent = 'None';
        }
    }

    digit1Buttons.forEach(button => {
        button.addEventListener('click', function () {
            digit1Buttons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            selectedDigit1 = this.getAttribute('data-digit1');
            selectedDigit1Text = this.textContent;

            update3DImage();
        });
    });

    digit2Buttons.forEach(button => {
        button.addEventListener('click', function () {
            digit2Buttons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            selectedDigit2 = this.getAttribute('data-digit2');
            selectedDigit2Text = this.textContent;

            update3DImage();
        });
    });

    digit3Buttons.forEach(button => {
        button.addEventListener('click', function () {
            digit3Buttons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');

            selectedDigit3 = this.getAttribute('data-digit3');
            selectedDigit3Text = this.textContent;

            update3DImage();
        });
    });

});
