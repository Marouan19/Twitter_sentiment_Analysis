/*!
    * Start Bootstrap - SB Admin v7.0.7 (https://startbootstrap.com/template/sb-admin)
    * Copyright 2013-2023 Start Bootstrap
    * Licensed under MIT (https://github.com/StartBootstrap/startbootstrap-sb-admin/blob/master/LICENSE)
    */
    // 
// Scripts
// 

window.addEventListener('DOMContentLoaded', event => {

    // Toggle the side navigation
    const sidebarToggle = document.body.querySelector('#sidebarToggle');
    if (sidebarToggle) {
        // Uncomment Below to persist sidebar toggle between refreshes
        // if (localStorage.getItem('sb|sidebar-toggle') === 'true') {
        //     document.body.classList.toggle('sb-sidenav-toggled');
        // }
        sidebarToggle.addEventListener('click', event => {
            event.preventDefault();
            document.body.classList.toggle('sb-sidenav-toggled');
            localStorage.setItem('sb|sidebar-toggle', document.body.classList.contains('sb-sidenav-toggled'));
        });
    }

});
function readFile(input) {
    if (input.files && input.files[0]) {
        const file = input.files[0];
        const fileType = file.type;

        // Check if the file type is CSV
        if (fileType !== 'text/csv') {
            alert('Please upload a CSV file.');
            removeUpload();
            return;
        }

        const reader = new FileReader();

        reader.onload = function(e) {
            $('.file-upload-wrap').hide();
            $('.file-upload-image').attr('src', e.target.result);
            $('.file-upload-content').show();
            $('.image-title').html(file.name);
        };

        reader.readAsDataURL(file);
    } else {
        removeUpload();
    }
}
function submitForm() {
    // Get the form element
    var form = document.getElementById('upload-form');

    // Create FormData object to collect form data
    var formData = new FormData(form);

    // Send an AJAX request to handle form submission
    fetch('/upload', {
        method: 'POST',
        body: formData
    })
        .then(response => {
            // Handle the response here (if needed)
            console.log(response);
        })
        .catch(error => {
            // Handle errors here (if needed)
            console.error('Error:', error);
        });
}
