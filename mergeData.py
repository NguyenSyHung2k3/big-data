import os
import json
import csv
from pyspark.sql import SparkSession
# folder_path = "./data"
# merged_data = []
# for filename in os.listdir(folder_path):
#     if(filename.endswith('json')):
#         file_path = os.path.join(folder_path, filename)
#         with open(file_path, 'r', encoding='utf-8') as f:
#             try:
#                 data = json.load(f)
#                 if isinstance(data, list):
#                     merged_data.extend(data)
#                 else: 
#                     merged_data.append(data)
#             except json.JSONDecodeError as e:
#                 print(f"Error decoding JSON in file {filename}: {e}")
# output_file = "merged.json"
# with open(output_file, 'w', encoding='utf-8') as f:
#     json.dump(merged_data, f, ensure_ascii=False, indent=4)

# print(f"All JSON files have been merged into {output_file}.")
# input_file = "merged.json"  # Thay bằng tên file JSON của bạn
# # Đường dẫn file CSV đầu ra
# output_file = "merged.csv"

# # Đọc dữ liệu từ file JSON
# with open(input_file, "r", encoding="utf-8") as json_file:
#     data = json.load(json_file)  # Giả sử file JSON chứa danh sách các đối tượng

# # Lấy danh sách các trường từ đối tượng đầu tiên
# if isinstance(data, list) and len(data) > 0:
#     fieldnames = data[0].keys()
# else:
#     raise ValueError("File JSON không chứa danh sách các đối tượng hợp lệ!")

# # Ghi dữ liệu vào file CSV
# with open(output_file, "w", encoding="utf-8", newline="") as csv_file:
#     writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
#     writer.writeheader()  # Ghi tiêu đề cột
#     writer.writerows(data)  # Ghi các dòng dữ liệu

# print(f"Chuyển đổi thành công! Dữ liệu được lưu tại {output_file}.")

#--------------


# Đọc dữ liệu từ file JSON
# import ijson
# import json
# from decimal import Decimal

# # Function to convert Decimal to a serializable type (float or str)
# def custom_serializer(obj):
#     if isinstance(obj, Decimal):
#         return float(obj)  # Or use str(obj) if you prefer string representation
#     raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# # Paths to input and output files
# input_file = "merged.json"
# output_file = "after_merged.json"

# # Read and filter JSON data using ijson
# with open(input_file, "r", encoding="utf-8") as file:
#     items = ijson.items(file, "item")
#     filtered_items = [item for item in items if "name" not in item]

# # Write the filtered data to a new JSON file with custom serialization
# with open(output_file, "w", encoding="utf-8") as file:
#     json.dump(filtered_items, file, indent=4, ensure_ascii=False, default=custom_serializer)

# print(f"Processed data saved to {output_file}")

import json
import csv
import ijson
import uuid
import random

# Paths to the filtered JSON file and the output CSV file
input_json_file  = "after_merged.json"  # Update with your file path
output_csv_file = "output_file.csv"          # Update with your desired output path

# Step 1: Read the JSON file
# with open(input_json_file, "r", encoding="utf-8") as file:
#     # Parse items in the JSON array
#     items = ijson.items(file, "item")

#     # Convert items to a list of dictionaries
#     data = list(items)  # For very large files, process rows directly in the loop below

# # Step 2: Write to CSV
# if len(data) > 0:  # Ensure there is data to process
#     # Extract headers dynamically from the first item
#     headers = data[0].keys()

#     # Open the CSV file for writing
#     with open(output_csv_file, "w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=headers)
#         writer.writeheader()  # Write the header row
#         writer.writerows(data)  # Write rows of data

#     print(f"CSV file saved to {output_csv_file}")
# else:
#     print("No data found in the JSON file.")
input_file = "after_merged.json"
output_file = "expanded_data.json"

# Đọc dữ liệu gốc
with open(input_file, "r", encoding="utf-8") as file:
    original_data = json.load(file)

# Hàm tạo nhiễu cho các giá trị số
def add_noise(value, scale=0.1):
    if isinstance(value, (int, float)):
        noise = random.uniform(-scale, scale) * value
        return round(value + noise, 6)
    return value

# Hàm tạo bản ghi mới dựa trên bản ghi cũ
def generate_new_record(record):
    new_record = record.copy()
    new_record["Track ID"] = str(uuid.uuid4())  # Tạo ID mới
    new_record["Track Name"] = f"{record['Track Name']} (Remix)"  # Thêm hậu tố vào tên bài hát
    new_record["Popularity"] = add_noise(record["Popularity"], scale=10)  # Thay đổi giá trị Popularity
    new_record["Energy"] = add_noise(record["Energy"], scale=0.05)  # Thay đổi giá trị Energy
    new_record["Tempo"] = add_noise(record["Tempo"], scale=2)  # Thay đổi giá trị Tempo
    return new_record

# Tạo thêm dữ liệu
expanded_data = original_data.copy()  # Bản gốc
for record in original_data:
    expanded_data.append(generate_new_record(record))  # Thêm bản ghi mới

# Ghi dữ liệu mở rộng vào file JSON mới
with open(output_file, "w", encoding="utf-8") as file:
    json.dump(expanded_data, file, indent=4, ensure_ascii=False)

print(f"Đã mở rộng dữ liệu lên gấp đôi và lưu tại {output_file}")
from decimal import Decimal
def add_noise(value, scale=0.1):
    if isinstance(value, (int, float)):
        noise = random.uniform(-scale, scale) * value
        return round(value + noise, 6)
    return value

# Function to generate a new record from an existing record
def generate_new_record(record):
    new_record = record.copy()
    new_record["Track ID"] = str(uuid.uuid4())  # Generate a new Track ID
    new_record["Track Name"] = f"{record['Track Name']} (Remix)"  # Modify Track Name
    new_record["Popularity"] = add_noise(record["Popularity"], scale=10)  # Add noise
    new_record["Energy"] = add_noise(record["Energy"], scale=0.05)
    new_record["Tempo"] = add_noise(record["Tempo"], scale=2)
    return new_record

# Custom JSON encoder to handle Decimal objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        return super().default(obj)

# Process the JSON file using ijson
with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
    # Start the JSON array
    outfile.write("[\n")
    items = ijson.items(infile, "item")
    first = True  # Track if it's the first item

    for item in items:
        # Write the original record
        if not first:
            outfile.write(",\n")
        json.dump(item, outfile, indent=4, ensure_ascii=False, cls=CustomJSONEncoder)

        # Write the new generated record
        outfile.write(",\n")
        new_record = generate_new_record(item)
        json.dump(new_record, outfile, indent=4, ensure_ascii=False, cls=CustomJSONEncoder)

        first = False

    # End the JSON array
    outfile.write("\n]")

print(f"Expanded data saved to {output_file}")