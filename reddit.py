from oauth import *
from random import choice

# List to store new posts
daily_posts = []
m_links = []
seen_posts = set()

# 📌 Tenta carregar os posts já vistos do arquivo
try:
    with open("seen_posts.txt", "r") as f:
        seen_posts.update(f.read().splitlines())
except FileNotFoundError:
    pass  # Se não existir o arquivo, apenas ignore

def subReddit(postlimit):
    x = (reddit.subreddit("Overwatch").hot(limit=postlimit), reddit.subreddit("Overwatch_Memes").hot(limit=postlimit))
    subreddit = choice(x)
    return subreddit

def extractContent():
    limit = 1  # Initial limit
    new_posts = []  # ✅ Agora armazenamos múltiplos posts novos

    while True:
        for sub in subReddit(limit):
            max_imgs = 0
            if not sub.stickied and sub.id not in seen_posts:
                post_data = {  # ✅ Agora criamos um dicionário para cada post
                    "title": sub.title,
                    "content": sub.selftext,
                    "url": f"https://www.reddit.com{sub.permalink}",
                    "s_img": '',
                    "m_img": list(),
                    "video": ''
                }

                if hasattr(sub, "preview"):
                    post_data["s_img"] = sub.preview["images"][0]["source"]["url"]
                if hasattr(sub, "gallery_data"):
                    for i in sub.gallery_data["items"]:
                        media_id = i["media_id"]
                        image_url = sub.media_metadata[media_id]["s"]["u"]
                        m_links.append(image_url)
                        max_imgs += 1
                        if max_imgs >= 4:
                            break
                    if m_links:
                        post_data["m_img"] = m_links[:]
                    m_links.clear()

                if sub.media and isinstance(sub.media, dict) and "reddit_video" in sub.media:
                    post_data["video"] = sub.media["reddit_video"]["fallback_url"]
                
                new_posts.append(post_data)  # ✅ Adicionamos à lista de posts novos
                seen_posts.add(sub.id)  # ✅ Marcamos como visto
                break  # Exit the loop after finding one new post
            else:
                limit += 1  # Increase the limit if the post is stickied or already seen

        if new_posts or limit > 100:  # Stop if we have new posts or the limit is too high
            break
    
    return new_posts  # ✅ Retorna a lista de posts novos

def debug_data(posts):
    if posts:
        for post in posts:
            print("\n🔹 Novo Post Encontrado:")
            for k, v in post.items():
                print(f"{k} = {v}")
        print()
    else:
        print("🚫 Nenhum post novo encontrado.\n")

if __name__ == "__main__":
    new_data = extractContent()
    if new_data:
        debug_data(new_data)
    else:
        print("⏳ Aguardando novos posts...\n")