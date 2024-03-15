
function showSection(section) {
    if (section === 'upload') {
        document.getElementById('upload-section').style.display = 'block';
        document.getElementById('db-connection-section').style.display = 'none';
    } else if (section === 'db') {
        document.getElementById('upload-section').style.display = 'none';
        document.getElementById('db-connection-section').style.display = 'block';
    }
}



function showSection(section) {
    if (section === 'upload') {
        document.getElementById('upload-section').style.display = 'block';
        document.getElementById('db-connection-section').style.display = 'none';
    } else if (section === 'db') {
        document.getElementById('upload-section').style.display = 'none';
        document.getElementById('db-connection-section').style.display = 'block';
    }
}

function handleFileUpload() {
    var fileInput = document.getElementById('fileInput');
    var uploadBtn = document.getElementById('uploadBtn');
    var feedbackDiv = document.getElementById('upload-feedback');
    var loadingDiv = document.getElementById('loading-upload');

    loadingDiv.style.display = 'inline-block';
    uploadBtn.disabled = true;

    var formData = new FormData();
    formData.append('file', fileInput.files[0]);

    fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                feedbackDiv.innerHTML = 'Deduplication successful! Download your results: <a href="' + data.download_path + '" download>Download Deduplicated File</a>';
            } else {
                feedbackDiv.innerHTML = 'Error: ' + data.error;
            }
        })
        .catch(error => {
            feedbackDiv.innerHTML = 'Error: ' + error.message;
        })
        .finally(() => {
            loadingDiv.style.display = 'none';
            uploadBtn.disabled = false;
        });
}

function handleDBConnection() {
    var dbtypeInput = document.getElementById('dbtype');
    var dbnameInput = document.getElementById('dbname');
    var userInput = document.getElementById('user');
    var passwordInput = document.getElementById('password');
    var hostInput = document.getElementById('host');
    var portInput = document.getElementById('port');
    var confidenceInput = document.getElementById('confidence');
    var tablenameInput = document.getElementById('tablename');
    var candidatenameInput = document.getElementById('candidatename');
    var fathernameInput = document.getElementById('fathername');
    var mothernameInput = document.getElementById('mothername');
    var connectBtn = document.getElementById('connectBtn');
    var feedbackDiv = document.getElementById('db-feedback');
    var loadingDiv = document.getElementById('loading-db');
    console.log(`${dbnameInput}, ${dbtypeInput}, ${userInput}, ${passwordInput}, ${hostInput}, ${portInput}, ${confidenceInput}, ${tablenameInput}, ${candidatenameInput}, ${fathernameInput}, ${mothernameInput}`);

    loadingDiv.style.display = 'inline-block';
    connectBtn.disabled = true;

    // Extract user-entered values
    var dbtype = dbtypeInput.value;

    var dbname = dbnameInput.value;
    var user = userInput.value;
    var password = passwordInput.value;
    var host = hostInput.value;
    var port = portInput.value;
    var confidence = confidenceInput.value;
    var tablename = tablenameInput.value;  // Add .value to get the input value
    var candidatename = candidatenameInput.value;
    var fathername = fathernameInput.value;
    var mothername = mothernameInput.value;
    console.log(`${dbname}, ${dbtype}, ${user}, ${password}, ${host}, ${port}, ${confidence}, ${tablename}, ${candidatename}, ${fathername}, ${mothername}`);


    // Use these values to connect to the database
    fetch('/connect_db', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: `dbtype=${dbtype}&dbname=${dbname}&user=${user}&password=${password}&host=${host}&port=${port}&confidence=${confidence}&tablename=${tablename}&candidatename=${candidatename}&fathername=${fathername}&mothername=${mothername}`,

    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            feedbackDiv.innerHTML = 'Database connection successful!';

            // Call Dedupe.py with the entered values
            callDedupeScript(dbtype,dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername);
        } else {
            feedbackDiv.innerHTML = 'Error: ' + data.error;
        }
    })
    .catch(error => {
        feedbackDiv.innerHTML = 'Error: ' + error.message;
    })
    .finally(() => {
        loadingDiv.style.display = 'none';
        connectBtn.disabled = false;
    });
}

// Function to call Dedupe.py with the entered values
function callDedupeScript(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername) {
    fetch('/run_dedupe', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            dbtype: dbtype,
            dbname: dbname,
            user: user,
            password: password,
            host: host,
            port: port,
            confidence: confidence.toString(),
            tablename: tablename,
            candidatename: candidatename,
            fathername: fathername,
            mothername: mothername
        }),
    })
    .then(response => {
        return response.json().then(data => {
            console.log(data); // Log the response data
            return { response, data }; // Return both the response and the data
        });
    })
    .then(({ response, data }) => {
        // Handle the response data as needed
        console.log(data);
    })
    .catch(error => {
        console.error('Error calling Dedupe.py:', error);
    });
}

// function handleDBConnection() {
//     var dbnameInput = document.getElementById('dbname');
//     var userInput = document.getElementById('user');
//     var passwordInput = document.getElementById('password');
//     var hostInput = document.getElementById('host');
//     var portInput = document.getElementById('port');
//     var connectBtn = document.getElementById('connectBtn');
//     var feedbackDiv = document.getElementById('db-feedback');
//     var loadingDiv = document.getElementById('loading-db');

//     loadingDiv.style.display = 'inline-block';
//     connectBtn.disabled = true;

//     // Extract user-entered values
//     var dbname = dbnameInput.value;
//     var user = userInput.value;
//     var password = passwordInput.value;
//     var host = hostInput.value;
//     var port = portInput.value;

//     // Use these values to connect to the database
//     fetch('/connect_db', {
//             method: 'POST',
//             headers: {
//                 'Content-Type': 'application/x-www-form-urlencoded',
//             },
//             body: `dbname=${dbname}&user=${user}&password=${password}&host=${host}&port=${port}`,
//         })
//         .then(response => response.json())
//         .then(data => {
//             if (data.success) {
//                 feedbackDiv.innerHTML = 'Database connection successful!';

//                 // Call Dedupe.py with the entered values
//                 callDedupeScript(dbname, user, password, host, port);
//             } else {
//                 feedbackDiv.innerHTML = 'Error: ' + data.error;
//             }
//         })
//         .catch(error => {
//             feedbackDiv.innerHTML = 'Error: ' + error.message;
//         })
//         .finally(() => {
//             loadingDiv.style.display = 'none';
//             connectBtn.disabled = false;
//         });
// }

// // Function to call Dedupe.py with the entered values
// function callDedupeScript(dbname, user, password, host, port) {
//     fetch('/run_dedupe', {
//         method: 'POST',
//         headers: {
//             'Content-Type': 'application/x-www-form-urlencoded',
//         },
//         body: `dbname=${dbname}&user=${user}&password=${password}&host=${host}&port=${port}`,
//     })
//     .then(response => response.json())
//     .then(data => {
//         // Handle the response from Dedupe.py as needed
//         console.log(data);
//     })
//     .catch(error => {
//         console.error('Error calling Dedupe.py:', error);
//     });
// }