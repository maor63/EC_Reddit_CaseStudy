import json

from src.reddit_handler import RedditHandler
import pickle
import datetime
import os

# initializing RedditHandler
out_folder = 'Data'
extract_post = False  # True if you want to extract Post data, False otherwise
extract_comment = True  # True if you want to extract Comment data, False otherwise
post_attributes = ['id', 'author', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score', 'selftext', 'stickied',
                   'subreddit', 'subreddit_id', 'title']  # default
comment_attributes = ['id', 'author', 'created_utc', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'body',
                      'score']  # default
my_handler = RedditHandler(out_folder, extract_post, extract_comment, post_attributes=post_attributes,
                           comment_attributes=comment_attributes)

# extracting posts' comments for each category and semester
categories = [
    # {'guncontrol': ['Firearms', 'antiwar', 'guncontrol', 'gunpolitics', 'guns', 'liberalgunowners']},
    # {'politics': ['Conservative', 'Libertarian', 'democrats', 'Republican', 'esist', 'MarchAgainstTrump']},
    # {'minority': ['Anarchism', 'MensRights', 'AgainstHateSubreddits', 'racism', 'metacanada', 'KotakuInAction']},
    {'PoliticalDiscussion': ['PoliticalDiscussion']},
]
semesters = [('01/01/2022', '01/02/2022'), ('01/02/2022', '01/03/2022'), ('01/03/2022', '01/04/2022')]
path = r'data/Categories_raw_data/PoliticalDiscussion_01-01-2022_13-03-2022/'

for cat in categories:
    for topic in cat:
        for semester in semesters:
            period0 = datetime.datetime.strptime(semester[0], "%d/%m/%Y").strftime("%d-%m-%Y")
            period1 = datetime.datetime.strptime(semester[1], "%d/%m/%Y").strftime("%d-%m-%Y")
            semester_dir_name = f'{topic}_{period0}_{period1}'
            semester_path = os.path.join(path, semester_dir_name, f'{topic}_{period0}_{period1}.pickle')
            print(semester_path)
            if os.path.exists(semester_path):
                with open(semester_path, "rb") as input_file:
                    post_ids_authors = pickle.load(
                        input_file)  # loading dict with post_id as key and author name as value
            else:
                post_ids_authors = {}
                for post_file in os.listdir(os.path.join(path, semester_dir_name)):
                    if post_file.endswith('.json'):
                        with open(os.path.join(path, semester_dir_name, post_file)) as p_json:
                            user_posts = json.load(p_json)
                        for post in user_posts['posts']:
                            post_ids_authors[post["id"]] = post['author']
                with open(semester_path, "wb") as input_file:
                    pickle.dump(post_ids_authors, input_file)

            # extracting user data
            start_date = semester[0]
            end_date = semester[1]
            my_handler.extract_comment_fromid(post_ids_authors, topic, start_date=start_date, end_date=end_date)
