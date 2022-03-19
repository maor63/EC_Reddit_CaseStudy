import datetime
from collections import defaultdict
import json
from src.reddit_handler import RedditHandler
from pathlib import Path
import os
from tqdm.auto import tqdm
import pandas as pd
import networkx as nx


def main():
    # initializing RedditHandler
    base_path = Path('data/Categories_raw_data/')
    topic = 'PoliticalDiscussion'
    min_comments = 20
    start_date = '01/01/2022'
    end_date = '13/03/2022'

    posts_data = {}
    post_edges = defaultdict(list)
    post_author_edges = defaultdict(list)
    comment_data = {}
    post_comments = defaultdict(list)

    out_folder = 'Data'
    extract_post = True  # True if you want to extract Post data, False otherwise
    extract_comment = True  # True if you want to extract Comment data, False otherwise
    post_attributes = ['id', 'author', 'author_fullname', 'created_utc', 'num_comments', 'over_18', 'is_self', 'score',
                       'selftext', 'stickied', 'subreddit', 'subreddit_id', 'title', 'permalink']  # default
    comment_attributes = ['id', 'author', 'author_fullname', 'created_utc', 'link_id', 'parent_id', 'subreddit',
                          'subreddit_id', 'body', 'score', 'permalink']  # default
    my_handler = RedditHandler(out_folder, extract_post, extract_comment, post_attributes=post_attributes,
                               comment_attributes=comment_attributes)

    period0 = datetime.datetime.strptime(start_date, "%d/%m/%Y").strftime("%d-%m-%Y")
    period1 = datetime.datetime.strptime(end_date, "%d/%m/%Y").strftime("%d-%m-%Y")
    semester_dir_name = f'{topic}_{period0}_{period1}'
    topic_data_path = base_path / semester_dir_name

    for semester_dir in tqdm(topic_data_path.iterdir()):
        if os.path.isdir(semester_dir):
            for user_json_file in tqdm(semester_dir.iterdir(), total=len(list(semester_dir.iterdir()))):
                if user_json_file.name.endswith('.json'):
                    with open(str(user_json_file)) as j:
                        user_json = json.load(j)
                    for post in user_json['posts']:
                        posts_data[post['id']] = post
                        comment_data[post['id']] = post
                    add_comments(user_json['comments'], comment_data, post_comments, post_edges)

    submission_rows = [post for post_id, post in posts_data.items()]
    pd.DataFrame(submission_rows).to_csv(topic_data_path / f'submissions_{semester_dir_name}.csv', index=False)
    comments_path = topic_data_path / 'comments'
    for post_id, post_data in tqdm(posts_data.items(), desc='create graphs'):
        post_comment_path = comments_path / post_id
        if len(post_comments[post_id]) >= min_comments and not post_comment_path.exists():
            print(post_id)
            if not post_comment_path.exists():
                os.makedirs(post_comment_path)

            with open(post_comment_path / 'post_data.json', 'w') as j:
                json.dump(post_data, j)

            missing_comments = get_missing_comments(comment_data, post_edges, post_id)
            while len(missing_comments) > 0:
                new_comments = my_handler.comment_request_API_comment_ids(missing_comments)
                if len(new_comments) == 0:
                    break
                add_comments(new_comments, comment_data, post_comments, post_edges)
                missing_comments = get_missing_comments(comment_data, post_edges, post_id)
                print(len(missing_comments))

            cascade_graph = nx.DiGraph()
            cascade_graph.add_edges_from(post_edges[post_id])
            nx.write_gml(cascade_graph, post_comment_path / 'cascade_graph.gml')
            nx.write_gexf(cascade_graph, post_comment_path / 'cascade_graph.gexf')
            author_edges = [(comment_data[u]['author'], comment_data[v]['author']) for u, v in post_edges[post_id] if
                            u not in missing_comments]
            pd.DataFrame(post_comments[post_id]).to_csv(post_comment_path / 'comments.csv', index=False)

            edge_df = pd.DataFrame(author_edges, columns=['src', 'dsc']).reset_index()
            weighted_edge_df = edge_df.groupby(['src', 'dsc']).count().reset_index()
            interaction_graph = nx.DiGraph()
            interaction_graph.add_weighted_edges_from(weighted_edge_df.itertuples(index=False, name=None))
            nx.write_gml(interaction_graph, post_comment_path / 'interaction_graph.gml')
            nx.write_gexf(interaction_graph, post_comment_path / 'interaction_graph.gexf')

    print('done')


def get_missing_comments(comment_data, post_edges, post_id):
    missing_comments = []
    for u, v in post_edges[post_id]:
        if u not in comment_data:
            missing_comments.append(u)
        if v not in comment_data:
            missing_comments.append(v)
    return missing_comments


def add_comments(comments, comment_data, post_comments, post_edges):
    for comment in comments:
        post_id = comment['link_id'].split('_')[-1]
        parent_id = comment['parent_id'].split('_')[-1]
        post_edges[post_id].append((parent_id, comment['id']))
        comment_data[comment['id']] = comment
        post_comments[post_id].append(comment)


if __name__ == "__main__":
    main()
