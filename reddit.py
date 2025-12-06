import requests
import logging
import time
from random import choice
from database import is_post_seen, mark_post_as_seen

# Configuração de logging
logger = logging.getLogger(__name__)

# Lista para armazenar links de imagens de galerias
m_links = []

def get_reddit_json(subreddit, limit=50):
    """
    Busca posts do Reddit usando o endpoint JSON público.
    Não requer API key ou autenticação.
    """
    url = f"https://old.reddit.com/r/{subreddit}/hot.json?limit={limit}&raw_json=1"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        logger.info(f"Buscando posts de r/{subreddit}...")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✓ {len(data['data']['children'])} posts obtidos de r/{subreddit}")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar r/{subreddit}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao processar JSON: {e}")
        return None

def subReddit(limit):
    """
    Escolhe aleatoriamente entre os subreddits disponíveis.
    Retorna os dados JSON do subreddit escolhido.
    """
    subreddits = ['Overwatch', 'Overwatch_Memes']
    selected = choice(subreddits)
    
    logger.info(f"Subreddit selecionado: r/{selected}")
    return get_reddit_json(selected, limit)

def extractContent():
    """
    Extrai conteúdo de posts do Reddit usando JSON público.
    Retorna uma lista com informações do primeiro post novo encontrado.
    """
    limit = 50  # Busca mais posts para aumentar chances de encontrar algo novo
    new_posts = []
    max_imgs = 0
    
    # Busca posts do subreddit
    data = subReddit(limit)
    
    if not data:
        logger.warning("Não foi possível obter dados do Reddit")
        return []
    
    # Processa cada post
    for post in data['data']['children']:
        try:
            post_data = post['data']
            
            # Pula posts fixados (stickied)
            if post_data.get('stickied', False):
                logger.debug(f"Pulando post fixado: {post_data.get('title', 'N/A')}")
                continue
            
            post_id = post_data['id']
            
            # Verifica se já vimos este post
            if is_post_seen(post_id):
                logger.debug(f"Post {post_id} já foi visto, pulando...")
                continue
            
            # Post novo encontrado!
            logger.info(f"✓ Post novo encontrado: {post_data.get('title', 'N/A')[:50]}...")
            
            # Monta estrutura de dados do post
            post_info = {
                "id": post_id,
                "title": post_data.get('title', ''),
                "content": post_data.get('selftext', ''),
                "url": f"https://www.reddit.com{post_data.get('permalink', '')}",
                "s_img": '',
                "m_img": [],
                "video": ''
            }
            
            # ========== IMAGENS ==========
            
            # Imagem única (preview)
            if 'preview' in post_data and 'images' in post_data['preview']:
                try:
                    image_url = post_data['preview']['images'][0]['source']['url']
                    # Decodifica HTML entities (&amp; -> &)
                    image_url = image_url.replace('&amp;', '&')
                    post_info["s_img"] = image_url
                    logger.info(f"  - Imagem única encontrada")
                except Exception as e:
                    logger.warning(f"Erro ao extrair preview image: {e}")
            
            # Galeria de imagens
            if post_data.get('is_gallery', False) and 'gallery_data' in post_data:
                try:
                    gallery_items = post_data['gallery_data']['items']
                    media_metadata = post_data.get('media_metadata', {})
                    
                    for item in gallery_items:
                        media_id = item['media_id']
                        
                        if media_id in media_metadata:
                            # Tenta pegar a URL da imagem
                            if 's' in media_metadata[media_id] and 'u' in media_metadata[media_id]['s']:
                                image_url = media_metadata[media_id]['s']['u']
                                image_url = image_url.replace('&amp;', '&')
                                m_links.append(image_url)
                                max_imgs += 1
                                
                                # Limita a 4 imagens
                                if max_imgs >= 4:
                                    break
                            else:
                                logger.warning(f"'u' não encontrado em media_metadata para media_id={media_id}")
                    
                    if m_links:
                        post_info["m_img"] = m_links[:]
                        logger.info(f"  - Galeria com {len(m_links)} imagens")
                    
                    m_links.clear()
                    
                except Exception as e:
                    logger.warning(f"Erro ao extrair galeria: {e}")
            
            # ========== VÍDEOS ==========
            
            if post_data.get('is_video', False):
                try:
                    if 'media' in post_data and post_data['media']:
                        if 'reddit_video' in post_data['media']:
                            # Passa a URL do post (necessário para yt-dlp processar)
                            post_info["video"] = f"https://www.reddit.com{post_data['permalink']}"
                            logger.info(f"  - Vídeo encontrado")
                            logger.debug(f"    URL do vídeo: {post_info['video']}")
                except Exception as e:
                    logger.warning(f"Erro ao extrair vídeo: {e}")
            
            # Adiciona post à lista
            new_posts.append(post_info)
            
            # Retorna apenas o primeiro post novo encontrado
            logger.info(f"Post preparado para processamento: ID={post_id}")
            return new_posts
            
        except Exception as e:
            logger.error(f"Erro ao processar post: {e}", exc_info=True)
            continue
    
    # Se chegou aqui, não encontrou posts novos
    logger.info("Nenhum post novo encontrado")
    return []

def debug_data(posts):
    """
    Função de debug para exibir informações dos posts.
    """
    if posts:
        for post in posts:
            print("\n🔹 Novo post encontrado:")
            for k, v in post.items():
                if k == "m_img" and v:
                    print(f"  {k} = [{len(v)} imagens]")
                elif k == "content" and len(str(v)) > 100:
                    print(f"  {k} = {str(v)[:100]}...")
                else:
                    print(f"  {k} = {v}")
        print()
    else:
        print("Nenhum post novo encontrado.\n")

# Rate limiting para evitar bloqueios
def rate_limit_sleep():
    """
    Adiciona um delay entre requisições para respeitar limites do Reddit.
    """
    time.sleep(2)  # 2 segundos entre requisições

if __name__ == "__main__":
    # Teste local
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)
    
    print("Testando extração de conteúdo do Reddit...\n")
    new_data = extractContent()
    
    if new_data:
        debug_data(new_data)
        
        # Marca posts como vistos (apenas em execução de teste)
        for post in new_data:
            mark_post_as_seen(post['id'])
            print(f"✓ Post {post['id']} marcado como visto")
    else:
        print("Aguardando novos posts...\n")