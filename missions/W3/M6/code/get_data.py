from datasets import load_dataset
dataset = load_dataset("McAuley-Lab/Amazon-Reviews-2023", "raw_review_All_Beauty", trust_remote_code=True)
dataset['full']
dataset['full'].to_json('/code/data.json')