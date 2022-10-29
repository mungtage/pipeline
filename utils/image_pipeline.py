import os
import requests
from tqdm import tqdm

def save_images(IMG_PATH, previous_images, current_images, df):
    create_images = current_images - previous_images
    current_images_json = df[df["desertionNo"].isin(create_images)][["desertionNo","popfile","happenDt"]].to_dict("records")

    for item_dict in tqdm(current_images_json):
        responds = requests.get(item_dict['popfile'])
        os.makedirs(f"{IMG_PATH}/{item_dict['happenDt']}", exist_ok=True)
        open(f"{IMG_PATH}/{item_dict['happenDt']}/{item_dict['desertionNo']}.jpg", "wb").write(responds.content)
    # return

def remove_images(IMG_PATH, previous_images, current_images, df):
    delete_images = previous_images - current_images
    delete_images_json = df[df["desertionNo"].isin(delete_images)][["desertionNo","happenDt"]].to_dict("records")
    for item_dict in tqdm(delete_images_json):
        
        os.remove(f"{IMG_PATH}/{item_dict['happenDt']}/{item_dict['desertionNo']}.jpg")

def image_pipeline(df, IMG_PATH):
    os.makedirs(IMG_PATH, exist_ok=True)
    previous_images = set([os.path.splitext(file)[0] for _, _, files in os.walk(IMG_PATH) for file in files])
    current_images = set(df["desertionNo"].to_list())
    
    save_images(IMG_PATH, previous_images, current_images, df)
    remove_images(IMG_PATH, previous_images, current_images, df)