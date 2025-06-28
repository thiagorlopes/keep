document.addEventListener('DOMContentLoaded', function() {
    const form = document.querySelector('form');
    const statusContainer = document.getElementById('status-container');

    form.addEventListener('submit', function(event) {
        event.preventDefault();
        statusContainer.innerHTML = '';

        const formData = new FormData(form);
        const email = formData.get('email');

        fetch('/download', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => { throw new Error(err.error || 'Server error') });
            }
            return response.json();
        })
        .then(data => {
            if (data.Statements && data.Statements.length > 0) {
                const zip = new JSZip();
                data.Statements.forEach(statement => {
                    const csvData = atob(statement.Content_Base64);
                    zip.file(statement.FileName, csvData);
                });

                zip.generateAsync({type:"blob"}).then(function(content) {
                    const url = window.URL.createObjectURL(content);
                    const a = document.createElement('a');
                    a.style.display = 'none';
                    a.href = url;
                    const safeEmail = email.replace(/[^a-zA-Z0-9]/g, '_');
                    a.download = `statements_${safeEmail}.zip`;
                    document.body.appendChild(a);
                    a.click();
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                });
            } else {
                statusContainer.innerHTML = `<div class="flash-error">No statements found for the provided email.</div>`;
            }
        })
        .catch(err => {
            statusContainer.innerHTML = `<div class="flash-error">${err.message}</div>`;
        });
    });
});
