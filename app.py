from flask import Flask, render_template, request, jsonify, send_from_directory
import shutil
import os
from werkzeug.utils import secure_filename
import subprocess

app = Flask(__name__)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def call_dedupe_script(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername):
    try:
        dedupe_script_path = 'Dedupe.py'  # Assuming Dedupe.py is in the same directory as app.py

        result = subprocess.run(["python", dedupe_script_path, dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername], capture_output=True, text=True)

        if result.returncode == 0:
            return {'success': True, 'message': result.stdout}
        else:
            return {'error': result.stderr}
    except Exception as e:
        return {'error': str(e)}

def dedupe_file(file_path, dedupe_script_path):
    try:
        subprocess.run(["python", dedupe_script_path, file_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Dedupe_File.py: {e}")

def dedupe_db(dbname, user, password, host, port, dedupe_script_path, confidence, tablename, candidatename, fathername, mothername):
    try:
        subprocess.run(["python", dedupe_script_path, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Dedupe_DB.py: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'})

    file = request.files['file']    

    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if file:
        try:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
            file.save(file_path)

            current_dir = os.getcwd()
            dedupe_script_path = os.path.join(current_dir, 'Dedupe1.py')  # Dedupe for file

            temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'temp')
            output_file = os.path.join(temp_dir, 'duplicates_output.xlsx')
            duplicates_download_path = '/download/temp_out/duplicates_output.xlsx'
            download_path = '/download/temp_out/duplicates_output.xlsx'

            # Pass file path to dedupe_file process
            dedupe_file(file_path, dedupe_script_path)

            return jsonify({'success': True, 'download_path': download_path, 'duplicates_download_path': duplicates_download_path})
        except Exception as e:
            return jsonify({'error': f'An error occurred: {str(e)}'})

@app.route('/connect_db', methods=['POST'])
def connect_db():
    try:
        dbtype = request.form.get('dbtype')
        dbname = request.form.get('dbname')
        user = request.form.get('user')
        password = request.form.get('password')
        host = request.form.get('host')
        port = request.form.get('port')
        confidence = request.form.get('confidence')
        tablename = request.form.get('tablename')
        candidatename = request.form.get('candidatename')
        fathername = request.form.get('fathername')
        mothername = request.form.get('mothername')

        if any(param is None for param in [dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername]):
            return jsonify({'error': 'One or more parameters are missing. Please provide all required parameters.'})

        result = call_dedupe_script(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername)

        if 'Database connection failed' in result.get('message', ''):
            return jsonify({'error': 'Database connection failed. Please check your details and try again.'})
        else:
            return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'An unexpected error occurred: {e}'})

@app.route('/download/temp_out/duplicates_output.xlsx')
def download_temp_out_file():
    try:
        dedupe_app_dir = '/home/abhay/Desktop/DA_Database'  # Change this to the actual path
        source_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'duplicates_output.xlsx')
        destination_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'temp_copy.xlsx')

        # Check if the file exists before copying
        if os.path.exists(source_path):
            shutil.copy(source_path, destination_path)
            print(f"Requested file path: {source_path}")
            print(f"File copied to: {destination_path}")
        else:
            return jsonify({'error': 'File not found'})

        return send_from_directory(os.path.join(dedupe_app_dir, 'temp', 'temp_out'), 'temp_copy.xlsx', as_attachment=True)
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'})


@app.route('/run_dedupe', methods=['POST'])
def run_dedupe():
    try:
        # Extract JSON data from the request
        data = request.json

        # Extract parameters from the JSON data
        dbtype = data['dbtype']
        dbname = data['dbname']
        user = data['user']
        password = data['password']
        host = data['host']
        port = data['port']
        confidence = data['confidence']
        tablename = data['tablename']
        candidatename = data['candidatename']
        fathername = data['fathername']
        mothername = data['mothername']

        # Call the function to run Dedupe.py script
        # Update the call to call_dedupe_script() function
        result = call_dedupe_script(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername)
        print(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername)

        print("Received parameters:")
        print(f"dbtype: {dbtype}, type: {type(dbtype)}")
        print(f"dbname: {dbname}, type: {type(dbname)}")
        print(f"user: {user}, type: {type(user)}")
        print(f"password: {password}, type: {type(password)}")
        print(f"host: {host}, type: {type(host)}")
        print(f"port: {port}, type: {type(port)}")
        print(f"confidence: {confidence}, type: {type(confidence)}")
        print(f"tablename: {tablename}, type: {type(tablename)}")
        print(f"candidatename: {candidatename}, type: {type(candidatename)}")
        print(f"fathername: {fathername}, type: {type(fathername)}")
        print(f"mothername: {mothername}, type: {type(mothername)}")



        # Return the result as JSON response
        return jsonify(result)
    except Exception as e:
        # Return error message
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True)






# from flask import Flask, render_template, request, jsonify
# import shutil
# import os
# from werkzeug.utils import secure_filename
# import subprocess

# app = Flask(__name__)

# UPLOAD_FOLDER = 'uploads'
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# def dedupe_process(file_path, dedupe_script_path):
#     try:
#         subprocess.run(["python", dedupe_script_path, file_path], check=True)
#     except subprocess.CalledProcessError as e:
#         print(f"Error running Dedupe.py: {e}")



# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file provided'})

#     file = request.files['file']

#     if file.filename == '':
#         return jsonify({'error': 'No selected file'})

#     if file:
#         try:
#             file_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
#             file.save(file_path)

#             current_dir = os.getcwd()
#             dedupe_script_path = os.path.join(current_dir, 'Dedupe.py')

#             # Get database connection details from frontend
#             dbname = request.form.get('dbname')
#             user = request.form.get('user')
#             password = request.form.get('password')
#             host = request.form.get('host')
#             port = request.form.get('port')

#             # Pass file path and database details to dedupe_process
#             dedupe_process(file_path, dedupe_script_path, dbname, user, password, host, port)

#             temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'temp')
#             output_file = os.path.join(temp_dir, 'deduplicated_output.xlsx')
#             duplicates_download_path = '/download/temp_out/duplicates_output.xlsx'
#             download_path = '/download/temp_out/deduplicated_output.xlsx'

#             return jsonify({'success': True, 'download_path': download_path, 'duplicates_download_path': duplicates_download_path})
#         except Exception as e:
#             return jsonify({'error': f'An error occurred: {str(e)}'})

# @app.route('/download/temp_out/duplicates_output.xlsx')
# def download_temp_out_file():
#     try:
#         dedupe_app_dir = '/home/abhay/Desktop/Dedupe_Application'  # Change this to the actual path
#         source_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'duplicates_output.xlsx')
#         destination_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'temp_copy.xlsx')

#         shutil.copy(source_path, destination_path)

#         print(f"Requested file path: {source_path}")
#         print(f"File copied to: {destination_path}")

#         return send_from_directory(os.path.join(dedupe_app_dir, 'temp', 'temp_out'), 'temp_copy.xlsx', as_attachment=True)
#     except Exception as e:
#         return jsonify({'error': f'An error occurred: {str(e)}'})

# if __name__ == '__main__':
#     app.run(debug=True)
















# from flask import Flask, render_template, request, jsonify, send_from_directory
# import shutil
# import os
# from werkzeug.utils import secure_filename
# import subprocess

# app = Flask(__name__)

# UPLOAD_FOLDER = 'uploads'
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file provided'})

#     file = request.files['file']

#     if file.filename == '':
#         return jsonify({'error': 'No selected file'})

#     # Retrieve database connection details from the frontend
#     dbname = request.form.get('dbname')
#     user = request.form.get('user')
#     password = request.form.get('password')
#     host = request.form.get('host')
#     port = request.form.get('port')

#     if file and dbname and user and password and host and port:
#         try:
#             # Use the retrieved database connection details in Dedupe.py
#             dedupe_script_path = os.path.join(os.getcwd(), 'Dedupe.py')
#             dedupe_process(file, dedupe_script_path, dbname, user, password, host, port)

#             temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'temp')
#             output_file = os.path.join(temp_dir, 'duplicates_output.xlsx')
#             duplicates_download_path = '/download/temp_out/duplicates_output.xlsx'
#             download_path = '/download/temp_out/duplicates_output.xlsx'  # Set the download path to the deduplicated file

#             return jsonify({'success': True, 'download_path': download_path, 'duplicates_download_path': duplicates_download_path})
#         except Exception as e:
#             return jsonify({'error': f'An error occurred: {str(e)}'})
#     else:
#         return jsonify({'error': 'Invalid request'})

# @app.route('/download/temp_out/duplicates_output.xlsx')
# def download_temp_out_file():
#     try:
#         dedupe_app_dir = '/home/abhay/Desktop/Dedupe_Application'  # Change this to the actual path
#         source_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'duplicates_output.xlsx')
#         destination_path = os.path.join(dedupe_app_dir, 'temp', 'temp_out', 'temp_copy.xlsx')

#         shutil.copy(source_path, destination_path)

#         print(f"Requested file path: {source_path}")
#         print(f"File copied to: {destination_path}")

#         return send_from_directory(os.path.join(dedupe_app_dir, 'temp', 'temp_out'), 'temp_copy.xlsx', as_attachment=True)
#     except Exception as e:
#         return jsonify({'error': f'An error occurred: {str(e)}'})

# def dedupe_process(file, dedupe_script_path, dbname, user, password, host, port):
#     try:
#         subprocess.run(["python", dedupe_script_path, file, dbname, user, password, host, port], check=True)
#     except subprocess.CalledProcessError as e:
#         print(f"Error running Dedupe.py: {e}")

# if __name__ == '__main__':
#     app.run(debug=True)