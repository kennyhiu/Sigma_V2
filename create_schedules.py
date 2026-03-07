import requests

url = "https://api.us-a.aws.sigmacomputing.com/v2/workbooks/XOy2picV8cor3ylaZNaOG/schedules"

payload = {
    "schedule": {
        "cronSpec": "29 16 1 * *",
        "timezone": "America/Chicago"
    },
    "target": [{ "email": "kenny.hiu@gmail.com" }],
    "configV2": {
        "title": "test 1",
        "messageBody": "test message",
        "exportAttachments": [
            {
                "formatOptions": { "type": "CSV" },
                "workbookExportSource": {
                    "type": "element",
                    "elementIds": ["DffvMMU8yS"]
                }
            }
        ],
        "exportName": "1234151251251251",
        "includeLink": True,
        "workbookVariant": { "tagId": "7d36ec92-ea2b-4f2c-9a4d-9828c5114a71" },
        "conditionOptions": { "type": "always" },
        "workbookSettings": { "controlValues": { "Facility-ID-Selection": {
                    "type": "number",
                    "value": 3188
                } } }
    },
    "description": "sdasdqweqweasd",
    "ownerId": "CRRZs7WYN9vdwOUaYSTsWtyvbFMYY"
}
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjczNjk2NzZkNjE2MzZmNmQ3MDc1NzQ2OTZlNjcyZDYzNjE2YzY5NjI2MTZlMGEifQ.eyJ0b2tlbl90eXBlIjoidjJhcGktYWNjZXNzIiwiZW52aXJvbm1lbnQiOiJwcm9kdWN0aW9uIiwiaWF0IjoxNzcyNDk3NjA5LCJleHAiOjE3NzI1MDEyMDksImF1ZCI6InByb2R1Y3Rpb24iLCJzdWIiOiJ5a0hCZ3locWhmMDJzajV2ZTBzQ3FNWDBBckN2SSIsInVpZCI6InlrSEJneWhxaGYwMnNqNXZlMHNDcU1YMEFyQ3ZJIiwib2lkIjoiN2E4YzM4YzUtYmQyOC00NWRmLWI1YmItNDhjYzE5MzRhYjlkIiwiZW1haWwiOiJraGl1QHBvd2VyZmxlZXQuY29tIiwiY2xpZW50X2lkIjoiZjA0Y2QzNjViZjc1YTExOWNjZDFiZmU0MjQ4ODIzMzRjNzFkMDhmOTkzMjI4OTc4ZmQ5MzJhZGRmYWI3YzAzNSIsImFwaUtleUlkIjoiZjA0Y2QzNjViZjc1YTExOWNjZDFiZmU0MjQ4ODIzMzRjNzFkMDhmOTkzMjI4OTc4ZmQ5MzJhZGRmYWI3YzAzNSIsImlzcyI6InNpZ21hY29tcHV0aW5nIn0.wbn8oXrp1jKMK3vl273O8qp2dYsDKhNQaeL0j0N9R3vigJkdNcSihdTWtlSq8ZlBHvZiU_V17p8lEpA1bjQkKRdYj3h1hVbQNWmCToyi16vAtJWNqPsn56DuwTkt0arwP8q4oHlXUy--GMdPReg_li7GYzfxDlJQ7i7BJropyTWueRjrV20HdDKbwwlfGihp9tnloJybOmIv7sNZ7pgKNc6xxU8un7AsXBKwhc7R2EdYTSLBAfLahtkUhS95oiVljOZ4XrpdEbBavSs_kTSxdln5LggP71kRMGXTZDR_2xUQu0tM7zaYDhZZR3KgX60eRELy2yfsT6xEhAFRsHkjWnKhM4f5EKSTYE8MI0NeUtcLWIFefZNy0w9HZt7c_DMN7Z4HQ6bK5O6eNPGzdS1EbQgW7JHIZS_LV9xJvWEyh_CYf_7ENmESe3GLARzVi6kex2Bdp2VQb0LYR7Fg8auWCzQwpVi40kf3Za9kSrXm1N3Ii427ip0CeS5SspY-9sS5QTOHINmjDBV7ZO_02Pi9yvpQMVB0aiZ7e0WIO98aaGZpjKkzVrUXWpdEbxJi1LQwd4HjsBSNLE8ZfiyfyvpKnsld1PLxAPsDZ5lJ4zy4tRHPo1yZpM90JCGjBOMB_v44DivZ5e8ssVYxSzUeEIysbn3jpGJ6hJ7SsoxH8IF5IgU"
}

response = requests.post(url, json=payload, headers=headers)

print(response.text)