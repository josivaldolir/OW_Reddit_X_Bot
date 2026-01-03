import requests
import logging
import os
from random import choice
from queue_manager import (
    initialize_queue_db, 
    add_json_batch, 
    get_next_unposted_post, 
    get_queue_stats
)

logger = logging.getLogger(__name__)

# Configurações do CCProxy (seu PC)
PROXY_HOST = os.getenv("PROXY_HOST", "")
PROXY_PORT = os.getenv("PROXY_PORT", "8080")
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")

def check_proxy_available():
    """
    Verifica se o CCProxy está online e acessível.
    Retorna True se disponível, False caso contrário.
    """
    if not PROXY_HOST:
        logger.warning("⚠️ PROXY_HOST não configurado")
        return False
    
    try:
        # Monta URL do proxy com autenticação
        if PROXY_USER and PROXY_PASS:
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
        
        proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        
        logger.info(f"🔍 Verificando CCProxy: {PROXY_HOST}:{PROXY_PORT}")
        
        # Tenta requisição simples
        response = requests.get(
            "https://www.reddit.com/r/test.json?limit=1",
            proxies=proxies,
            timeout=5
        )
        
        if response.status_code == 200:
            logger.info("✅ CCProxy DISPONÍVEL!")
            return True
        else:
            logger.warning(f"⚠️ CCProxy respondeu com status {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        logger.warning("⏱️ CCProxy: Timeout (PC pode estar desligado)")
        return False
    except requests.exceptions.ProxyError:
        logger.warning("🔌 CCProxy: Erro de proxy (verifique usuário/senha)")
        return False
    except requests.exceptions.ConnectionError:
        logger.warning("🔌 CCProxy: Conexão recusada (PC desligado ou CCProxy não rodando)")
        return False
    except Exception as e:
        logger.warning(f"⚠️ Erro ao verificar CCProxy: {e}")
        return False

def fetch_posts_from_reddit(subreddit, limit=50):
    """
    Busca posts do Reddit usando CCProxy.
    Retorna lista de posts ou None se falhar.
    """
    # Monta URL do proxy com autenticação
    if PROXY_USER and PROXY_PASS:
        proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    else:
        proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
    
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    # URLs para tentar
    urls = [
        f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}",
        f"https://www.reddit.com/r/{subreddit}.json?limit={limit}",
        f"https://old.reddit.com/r/{subreddit}/hot.json?limit={limit}",
    ]
    
    for url_index, url in enumerate(urls, 1):
        try:
            logger.info(f"🌐 Tentativa {url_index}/{len(urls)}: {url}")
            
            response = requests.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=20
            )
            response.raise_for_status()
            
            data = response.json()
            posts = []
            
            # Processa TODOS os posts do JSON
            for post in data['data']['children']:
                post_data = post['data']
                
                # Pula posts fixados
                if post_data.get('stickied', False):
                    continue
                
                post_info = {
                    "id": post_data['id'],
                    "title": post_data.get('title', ''),
                    "content": post_data.get('selftext', ''),
                    "url": f"https://www.reddit.com{post_data.get('permalink', '')}",
                    "s_img": '',
                    "m_img": [],
                    "video": '',
                    "video_fallback_url": ''
                }
                
                # Imagem única
                if 'preview' in post_data and 'images' in post_data['preview']:
                    try:
                        image_url = post_data['preview']['images'][0]['source']['url']
                        post_info["s_img"] = image_url.replace('&amp;', '&')
                    except:
                        pass
                
                # Galeria
                if post_data.get('is_gallery') and 'gallery_data' in post_data:
                    try:
                        images = []
                        for item in post_data['gallery_data']['items'][:4]:
                            media_id = item['media_id']
                            if media_id in post_data.get('media_metadata', {}):
                                if 's' in post_data['media_metadata'][media_id]:
                                    if 'u' in post_data['media_metadata'][media_id]['s']:
                                        img_url = post_data['media_metadata'][media_id]['s']['u']
                                        images.append(img_url.replace('&amp;', '&'))
                        post_info["m_img"] = images
                    except Exception as e:
                        logger.debug(f"Erro ao extrair galeria: {e}")
                
                # Vídeo
                if post_data.get('is_video') and 'media' in post_data:
                    if post_data['media'] and 'reddit_video' in post_data['media']:
                        post_info["video"] = f"https://www.reddit.com{post_data['permalink']}"
                        post_info["video_fallback_url"] = post_data['media']['reddit_video'].get('fallback_url', '')
                
                posts.append(post_info)
            
            logger.info(f"✅ {len(posts)} posts obtidos de r/{subreddit}!")
            return posts
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 402:
                logger.warning(f"❌ Erro 402 na tentativa {url_index} - proxy bloqueou endpoint")
                continue
            else:
                logger.warning(f"❌ HTTP {e.response.status_code} na tentativa {url_index}")
                continue
        except Exception as e:
            logger.warning(f"❌ Falha na tentativa {url_index}: {e}")
            continue
    
    logger.error("❌ Todas as tentativas falharam via CCProxy")
    return None

def extractContent():
    """
    Sistema otimizado de extração:
    
    1. Verifica se CCProxy está disponível
    2. Se SIM: busca 50 posts e adiciona como 1 JSON batch (máx 2 no DB)
    3. Se NÃO: busca da fila existente
    4. Varre JSON inteiro procurando post não visto
    5. Se JSON esgota, remove e passa para próximo
    6. Retorna sempre 1 post novo (ou lista vazia se não houver)
    """
    # Inicializa DB
    initialize_queue_db()
    
    # Mostra estatísticas
    stats = get_queue_stats()
    logger.info("=" * 60)
    logger.info(f"📊 Status da Fila:")
    logger.info(f"   Batches armazenados: {stats['batches_count']}/{MAX_JSON_BATCHES}")
    logger.info(f"   Posts disponíveis: {stats['available_posts']}")
    logger.info(f"   Total postados: {stats['posted_total']}")
    
    if stats['batches']:
        for batch in stats['batches']:
            logger.info(f"   • Batch #{batch['batch_id']}: r/{batch['subreddit']} - {batch['remaining']}/{batch['total']} restantes")
    
    logger.info("=" * 60)
    
    # Verifica se CCProxy está disponível
    proxy_available = check_proxy_available()
    
    if proxy_available and stats['batches_count'] < MAX_JSON_BATCHES:
        logger.info("🟢 MODO ONLINE: Buscando novos posts via CCProxy...")
        
        # Escolhe subreddit aleatório
        subreddits = ['Overwatch', 'Overwatch_Memes']
        subreddit = choice(subreddits)
        
        logger.info(f"🎲 Subreddit selecionado: r/{subreddit}")
        
        # Busca posts
        posts = fetch_posts_from_reddit(subreddit, limit=50)
        
        if posts:
            # Adiciona como 1 batch (FIFO automático se já tiver 2)
            batch_id = add_json_batch(posts, subreddit)
            logger.info(f"💾 Batch #{batch_id} salvo com {len(posts)} posts")
            
            # Atualiza estatísticas
            stats = get_queue_stats()
            logger.info(f"📊 Fila atualizada: {stats['batches_count']} batch(es), {stats['available_posts']} posts disponíveis")
        else:
            logger.warning("⚠️ Falha ao buscar novos posts")
    
    elif proxy_available and stats['batches_count'] >= MAX_JSON_BATCHES:
        logger.info(f"⏸️ Já temos {MAX_JSON_BATCHES} batches salvos (máximo), usando fila existente")
    
    elif not proxy_available:
        logger.info("🔴 MODO OFFLINE: CCProxy indisponível, usando fila existente")
    
    # Busca próximo post não visto (varre todos os batches)
    logger.info("🔍 Procurando próximo post não visto...")
    batch_id, post = get_next_unposted_post()
    
    if post:
        logger.info(f"✨ Post encontrado no batch #{batch_id}")
        logger.info(f"📤 Título: {post['title'][:70]}...")
        return [post]
    else:
        logger.error("❌ FILA VAZIA! Nenhum post novo disponível.")
        logger.error("   Aguardando CCProxy ficar online para buscar mais posts...")
        return []

def debug_data(posts):
    """Função de debug"""
    if posts:
        for post in posts:
            print("\n🔹 Post selecionado:")
            print(f"  ID: {post['id']}")
            print(f"  Título: {post['title'][:70]}...")
            print(f"  URL: {post['url']}")
            if post['s_img']:
                print(f"  Imagem: Sim")
            if post['m_img']:
                print(f"  Galeria: {len(post['m_img'])} imagens")
            if post['video']:
                print(f"  Vídeo: Sim")
        print()
    else:
        print("\n❌ Nenhum post disponível.\n")

# Importa MAX_JSON_BATCHES
from queue_manager import MAX_JSON_BATCHES

if __name__ == "__main__":
    # Teste local
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("Sistema Otimizado de Proxy Local com CCProxy")
    print(f"Máximo de batches: {MAX_JSON_BATCHES}")
    print("=" * 60)
    print()
    
    posts = extractContent()
    debug_data(posts)