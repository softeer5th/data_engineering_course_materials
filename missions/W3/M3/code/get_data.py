import requests

url = "https://gutenberg.net.au/ebooks01/0100021.txt"
file_name = "/code/data.txt"
print("Download")
response = requests.get(url)
if response.status_code == 200:
    with open(file_name, "w") as file:
        file.write(response.text)
