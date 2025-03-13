import base64

from fastapi import UploadFile

from scripts.constants.general_constants import GeneralConstants
import os
import csv
import datetime
import json
import tarfile
from bson import ObjectId
from scripts.logger.log_module import logger as log





class CommonUtil:

    def list_of_dicts_to_csv(self, data):
        try:
            file_name = str(datetime.datetime.now().strftime(GeneralConstants.date_time_format_ymd_hms)) + ".csv"
            save_path = GeneralConstants.export_data_path

            if not os.path.exists(save_path):
                os.makedirs(save_path)

            file_path = os.path.join(save_path, f"{file_name}")
            with open(file_path, 'w', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            self.delete_oldest_file_if_more_than_10_files(save_path)
            return file_path, file_name
        except Exception as e:
            raise e
            # return f"Exception when fetching list_of_dicts_to_csv data:{str(e)}"

    def list_of_dicts_to_json(self, data, suffix=""):
        try:
            converted_data = self.convert_objectid(data)
            file_name = str(datetime.datetime.now().strftime(GeneralConstants.date_time_format_ymd_hms)) + suffix +  ".json"
            save_path = GeneralConstants.export_data_path

            if not os.path.exists(save_path):
                os.makedirs(save_path)

            file_path = os.path.join(save_path, f"{file_name}")
            with open(file_path, 'w') as json_file:
                json.dump(converted_data, json_file, indent=4)

            self.delete_oldest_file_if_more_than_10_files(save_path)
            return file_path, file_name
        except Exception as e:
            log.error(f"Exception when creating json file : {str(e)}")
            raise e

    def compress_to_tar_gz(self, json_file_paths, suffix):
        try:
            file_name = str(datetime.datetime.now().strftime(GeneralConstants.date_time_format_ymd_hms)) + suffix +  ".tar.gz"
            tar_file_path = os.path.join(GeneralConstants.export_data_path, file_name)
            # Create tar.gz file
            with tarfile.open(tar_file_path, "w:gz") as tar:
                for json_file_path in json_file_paths:
                    # Add each JSON file to the tar file with its original name
                    arcname = os.path.basename(json_file_path)
                    tar.add(json_file_path, arcname=arcname)

            return tar_file_path, file_name
        except Exception as e:
            log.error(f"Exception when creating tar file : {str(e)}")
            raise e

    def get_record_count_from_file(self, save_path):
        try:
            with open(save_path, 'r') as file:
                data = json.load(file)
                return len(data)
        except Exception as e:
            log.error(f"Exception while counting records in file : {str(e)}")
            print(f"Error reading file {save_path}: {str(e)}")
            raise e

    def convert_objectid(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, bytes):
            # Convert bytes to base64 string
            return base64.b64encode(obj).decode('utf-8')
        # elif isinstance(obj, bytes):
        #     return obj.decode('utf-8')
        elif isinstance(obj, dict) and "$binary" in obj:
            # Directly return the base64 value without decoding
            return obj["base64"]

        elif isinstance(obj, dict):
            return {k: self.convert_objectid(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_objectid(item) for item in obj]
        return obj


    def read_json_file(self, uploaded_file: UploadFile):
        try:
            # Use the uploaded file's file-like interface
            data = json.load(uploaded_file.file)
            return data
        except Exception as e:
            log.error(f"Exception when reading json file: {str(e)}")
            raise e

    @staticmethod
    def delete_oldest_file_if_more_than_10_files(directory_path):
        try:
            files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
            if len(files) > 10:
                # Sort files by modification time
                files.sort(key=lambda x: os.path.getmtime(os.path.join(directory_path, x)))
                oldest_file = files[0]
                os.remove(os.path.join(directory_path, oldest_file))

        except Exception as e:
            log.exception(f"Exception while delete_oldest_file_if_more_than_10_files :{str(e)}")
            raise e


