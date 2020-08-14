import io
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Returns an authenticated pydrive object
def authenticate_google_drive():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("google_drive_credentials.txt")
    drive = GoogleDrive(gauth)
    return drive

# Upload a pandas df to a csv file in Google Drive
def upload_df_to_gd(file_name, df, folder_id, retries=0):
    try:
        drive = authenticate_google_drive()
        stream = io.StringIO()
        df.to_csv(stream)
        file_to_upload = drive.CreateFile({'title': file_name, 'mimeType': 'text/csv',
                                           "parents": [{"kind": "drive#fileLink", "id": folder_id}]})
        file_to_upload.SetContentString(stream.getvalue())
        file_to_upload.Upload()
    except Exception as err:
        max_retries = 5
        if retries < max_retries:
            return upload_df_to_gd(file_name, df, folder_id, retries+1)
        else:
            print(f"Error uploading {file_name} to Google Drive folder {folder_id}:\n {err}\n\n")
