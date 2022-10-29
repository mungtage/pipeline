import os
import requests
from tqdm import tqdm

def save_images(IMG_PATH, previous_images, current_images, df):
    create_images = current_images - previous_images
    current_images_json = df[df["desertionNo"].isin(create_images)][["desertionNo","popfile","happenDt"]].to_dict("records")

    for item_dict in tqdm(current_images_json):
        responds = requests.get(item_dict['popfile'])
        open(f"{IMG_PATH}/{item_dict['desertionNo']}_{item_dict['happenDt']}.jpg", "wb").write(responds.content)
    # return

def remove_images(IMG_PATH, previous_images, current_images, df):
    delete_images = previous_images - current_images
    delete_images_json = df[df["desertionNo"].isin(delete_images)][["desertionNo","happenDt"]].to_dict("records")
    for item_dict in tqdm(delete_images_json):
        os.makedirs(f"{IMG_PATH}/{item_dict['happenDt']}", exist_ok=True)
        os.remove(f"{IMG_PATH}/{item_dict['happenDt']}/{item_dict['desertionNo']}.jpg")

def image_pipeline(df, IMG_PATH):
    os.makedirs(IMG_PATH, exist_ok=True)
    previous_images = set([os.path.splitext(os.path.basename(path))[0] for path in os.listdir(IMG_PATH)])
    current_images = set(df["desertionNo"].to_list())
    
    save_images(IMG_PATH, previous_images, current_images, df)
    remove_images(IMG_PATH, previous_images, current_images, df)