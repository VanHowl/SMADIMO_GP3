import json
from collections import defaultdict
import numpy as np
import pandas as pd

def load_business_data(file_path):
    rating_groups = defaultdict(list)
    rating_bins = np.arange(2.0, 5.1, 0.5)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            business = json.loads(line)
            categories = business.get('categories', '')
            stars = business.get('stars', 0)

            if 'restaurants' in categories.lower():
                for i in range(len(rating_bins)-1):
                    if rating_bins[i] <= stars < rating_bins[i+1]:
                        rating_groups[i].append(business)
                        break
    
    top_restaurants = []
    reviews_per_group = 75 // len(rating_groups) + 1
    
    for group in rating_groups.values():
        group.sort(key=lambda x: x.get('review_count', 0), reverse=True)
        top_restaurants.extend(group[:min(reviews_per_group, len(group))])
        
        if len(top_restaurants) >= 75:
            top_restaurants = top_restaurants[:75]
            break
    
    return top_restaurants

def save_businesses_to_csv(businesses, output_path):
    df = pd.DataFrame(businesses)

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if not pd.isna(x) else '')

    df.to_csv(output_path, index=False, encoding='utf-8')

def load_reviews_for_businesses(review_file, business_ids):
    reviews_by_business = defaultdict(list)
    business_ids = set(business_ids)
    
    try:
        with open(review_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                try:
                    review = json.loads(line)
                    if review['business_id'] in business_ids:
                        reviews_by_business[review['business_id']].append(review)
                    
                    if (i + 1) % 100000 == 0:
                        print(f"Обработано {i+1} отзывов")
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"Файл не найден: {review_file}")
        return {}
    
    return reviews_by_business

def select_reviews_for_business(reviews):
    
    stars_groups = defaultdict(list)
    for review in reviews:
        stars = review.get('stars')
        if stars is not None and 0 <= stars <= 5:
            stars_groups[stars].append(review)
    
    selected_reviews = []
    for star, group in stars_groups.items():
        group.sort(key=lambda x: x.get('useful', 0), reverse=True)
        
        group_ratio = len(group) / len(reviews)
        num_to_select = max(1, round(group_ratio * 130))
        
        high_useful_count = int(0.6 * num_to_select)
        high_useful = group[:high_useful_count] if high_useful_count > 0 else []
        
        low_useful_count = num_to_select - high_useful_count
        if low_useful_count > 0 and len(group) > high_useful_count:
            low_useful = group[-low_useful_count:]
        else:
            low_useful = []
        
        selected_reviews.extend(high_useful + low_useful)
    
    return selected_reviews[:130]

def save_reviews_to_csv(reviews, output_path):
    df = pd.DataFrame(reviews)

    if 'text' in df.columns:
        df['text'] = df['text'].str.replace('\n', ' ').str.replace('\r', ' ')

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if not pd.isna(x) else '')

    df = df.fillna('')

    df.to_csv(output_path, index=False, encoding='utf-8')

def main():
    business_file = 'yelp_academic_dataset_business.json'
    review_file = 'yelp_academic_dataset_review.json'
    businesses_output = 'top_75_restaurants.csv'
    reviews_output = 'selected_restaurant_reviews_75.csv'

    top_restaurants = load_business_data(business_file)
    
    stars_distribution = defaultdict(int)
    for business in top_restaurants:
        stars = business.get('stars', 0)
        stars_distribution[stars] += 1
    
    business_ids = {b['business_id'] for b in top_restaurants}
    save_businesses_to_csv(top_restaurants, businesses_output)
    print(f"Сохранено {len(top_restaurants)} ресторанов в {businesses_output}")
    
    business_reviews = load_reviews_for_businesses(review_file, business_ids)
    
    review_counts = {bid: len(reviews) for bid, reviews in business_reviews.items()}
    print(f"\nЗагружены отзывы для {len(review_counts)} ресторанов")
    
    all_selected_reviews = []
    processed = 0
    
    for business_id, reviews in business_reviews.items():
        selected = select_reviews_for_business(reviews)
        all_selected_reviews.extend(selected)
        processed += 1
        print(f"Обработано ресторанов: {processed}/{len(business_ids)} - Отзывов: {len(selected)}")
    save_reviews_to_csv(all_selected_reviews, reviews_output)
    print(f"Сохранено {len(all_selected_reviews)} отзывов в {reviews_output}")
        
    print(f"Всего отзывов: {len(all_selected_reviews)}")