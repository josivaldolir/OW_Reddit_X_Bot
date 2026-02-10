import requests
import logging
import os
import re
from random import choice
from bs4 import BeautifulSoup
from queue_manager import (
    initialize_queue_db, 
    add_json_batch, 
    get_next_unposted_post, 
    get_queue_stats,
    MAX_JSON_BATCHES
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
        
        # Tenta requisição simples com HTML
        response = requests.get(
            "https://www.reddit.com/r/test/",
            proxies=proxies,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=15
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

def extract_post_id_from_url(url):
    """Extrai o ID do post da URL do Reddit"""
    match = re.search(r'/comments/([a-z0-9]+)/', url)
    return match.group(1) if match else None

def parse_reddit_html(html_content):
    """
    Faz o parsing do HTML do Reddit e extrai informações dos posts.
    Retorna lista de dicionários com dados dos posts.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    posts = []
    filtered_count = 0
    
    # Reddit usa diferentes estruturas dependendo se é new/old Reddit
    # Vamos tentar ambas as estruturas
    
    # Estrutura 1: shreddit-post (new Reddit)
    post_elements = soup.find_all('shreddit-post')
    
    # Estrutura 2: div com data-context="listing" (old Reddit fallback)
    if not post_elements:
        post_elements = soup.find_all('div', {'data-context': 'listing'})
    
    # Estrutura 3: thing data-type="link" (old Reddit)
    if not post_elements:
        post_elements = soup.find_all('div', class_=lambda x: x and 'thing' in x)
    
    logger.info(f"🔍 Encontrados {len(post_elements)} elementos de post no HTML")
    
    for post_elem in post_elements:
        try:
            post_info = extract_post_data(post_elem, soup)
            if post_info and post_info.get('id'):
                posts.append(post_info)
            elif post_info is None:
                filtered_count += 1
        except Exception as e:
            logger.debug(f"Erro ao processar post: {e}")
            continue
    
    if filtered_count > 0:
        logger.info(f"🚫 {filtered_count} posts filtrados (anúncios/promocionais)")
    
    return posts

def extract_post_data(post_elem, soup):
    """
    Extrai dados de um elemento de post do Reddit.
    Suporta múltiplas estruturas HTML (new/old Reddit).
    """
    post_info = {
        "id": '',
        "title": '',
        "content": '',
        "url": '',
        "s_img": '',
        "m_img": [],
        "video": '',
        "video_fallback_url": ''
    }
    
    def fix_url(url):
        """Corrige URLs que vêm sem protocolo"""
        if not url:
            return ''
        if url.startswith('//'):
            return 'https:' + url
        if not url.startswith('http'):
            return 'https://' + url
        return url
    
    def get_high_res_image_url(preview_url):
        """
        Converte URL de preview (baixa resolução) para alta resolução.
        
        Preview:  https://preview.redd.it/abc123.png?width=140&height=78&...
        Alta res: https://i.redd.it/abc123.png
        
        ou
        
        Preview:  https://preview.redd.it/abc123.png?width=140&...
        Alta res: https://preview.redd.it/abc123.png (sem query params)
        """
        if not preview_url:
            return ''
        
        # Remove query parameters (?width=140&...)
        if '?' in preview_url:
            base_url = preview_url.split('?')[0]
        else:
            base_url = preview_url
        
        # Tenta converter preview.redd.it → i.redd.it (imagem original)
        if 'preview.redd.it' in base_url:
            high_res_url = base_url.replace('preview.redd.it', 'i.redd.it')
            logger.debug(f"Converted to high-res: {high_res_url}")
            return high_res_url
        
        return base_url
    
    def extract_gallery_images(post_elem):
        """
        Extrai imagens de galerias (múltiplas imagens).
        Retorna lista de URLs em alta resolução.
        """
        images = []
        
        # Procura por links de galeria
        gallery_links = post_elem.find_all('a', href=True)
        
        for link in gallery_links:
            href = link.get('href', '')
            
            # URLs de imagem direta
            if any(domain in href for domain in ['i.redd.it', 'preview.redd.it']):
                if any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif']):
                    high_res = get_high_res_image_url(href)
                    if high_res and high_res not in images:
                        images.append(high_res)
            
            # Procura por imagens dentro de links
            img_tags = link.find_all('img')
            for img in img_tags:
                src = img.get('src', '')
                if src and any(domain in src for domain in ['i.redd.it', 'preview.redd.it']):
                    high_res = get_high_res_image_url(src)
                    if high_res and high_res not in images:
                        images.append(high_res)
        
        return images[:4]  # Máximo 4 imagens (limite do Twitter)
    
    # Detecta se é new Reddit (shreddit-post) ou old Reddit
    is_new_reddit = post_elem.name == 'shreddit-post'
    
    if is_new_reddit:
        # NEW REDDIT (shreddit-post)
        post_info['id'] = post_elem.get('id', '').replace('t3_', '')
        post_info['title'] = post_elem.get('post-title', '')
        
        # Permalink
        permalink = post_elem.get('permalink', '')
        if permalink:
            post_info['url'] = f"https://www.reddit.com{permalink}"
        
        # Selftext (conteúdo do post)
        content_html = post_elem.get('content-href', '')
        if content_html:
            post_info['content'] = content_html[:500]  # Limita tamanho
        
        # Imagem única
        thumbnail = post_elem.get('thumbnail', '')
        if thumbnail and 'redd.it' in thumbnail:
            high_res = get_high_res_image_url(thumbnail)
            post_info['s_img'] = fix_url(high_res)
        
        # Galeria (múltiplas imagens)
        gallery_images = extract_gallery_images(post_elem)
        if gallery_images:
            post_info['m_img'] = [fix_url(img) for img in gallery_images]
            # Se tem galeria, limpa s_img para evitar duplicação
            if post_info['s_img'] and post_info['s_img'] in post_info['m_img']:
                post_info['s_img'] = ''
        
        # Vídeo
        if post_elem.get('is-video') == 'true':
            post_info['video'] = post_info['url']
            
    else:
        # OLD REDDIT (div.thing)
        # ID do post
        post_id = post_elem.get('data-fullname', '').replace('t3_', '')
        if not post_id:
            post_id = post_elem.get('id', '').replace('thing_t3_', '')
        post_info['id'] = post_id
        
        # Título
        title_elem = post_elem.find('a', class_='title')
        if not title_elem:
            title_elem = post_elem.find('p', class_='title')
        
        if title_elem:
            post_info['title'] = title_elem.get_text(strip=True)
            
        # URL/Permalink
        permalink = post_elem.get('data-permalink', '')
        if permalink:
            post_info['url'] = f"https://www.reddit.com{permalink}"
        elif title_elem and title_elem.get('href'):
            href = title_elem.get('href')
            if href.startswith('/r/'):
                post_info['url'] = f"https://www.reddit.com{href}"
            else:
                post_info['url'] = href
        
        # Selftext/conteúdo
        expando = post_elem.find('div', class_='expando')
        if expando:
            usertext = expando.find('div', class_='usertext-body')
            if usertext:
                post_info['content'] = usertext.get_text(strip=True)[:500]
        
        # Imagem única
        thumbnail = post_elem.get('data-thumbnail', '')
        if thumbnail and 'redd.it' in thumbnail and thumbnail not in ['self', 'default', 'nsfw', 'spoiler']:
            high_res = get_high_res_image_url(thumbnail)
            post_info['s_img'] = fix_url(high_res)
        
        # Tenta encontrar imagem em preview (se não achou ainda)
        if not post_info['s_img']:
            preview = post_elem.find('a', class_='thumbnail')
            if preview:
                img = preview.find('img')
                if img and img.get('src'):
                    src = img.get('src')
                    if 'redd.it' in src:
                        high_res = get_high_res_image_url(src)
                        post_info['s_img'] = fix_url(high_res)
        
        # Galeria (múltiplas imagens)
        gallery_images = extract_gallery_images(post_elem)
        if gallery_images:
            post_info['m_img'] = [fix_url(img) for img in gallery_images]
            # Se tem galeria, limpa s_img para evitar duplicação
            if post_info['s_img'] and post_info['s_img'] in post_info['m_img']:
                post_info['s_img'] = ''
        
        # Vídeo (is-video ou domain)
        domain = post_elem.get('data-domain', '')
        is_video = post_elem.get('data-is-video', 'false') == 'true'
        
        if is_video or domain == 'v.redd.it':
            post_info['video'] = post_info['url']
    
    # Pula posts fixados (stickied)
    if post_elem.get('data-stickied') == 'true' or post_elem.get('stickied') == 'true':
        return None
    
    # FILTRO: Pula posts promocionais (de usuários, não de subreddits)
    # Anúncios têm URL tipo: /user/NOME/comments/...
    # Posts legítimos têm URL tipo: /r/SUBREDDIT/comments/...
    if post_info['url']:
        if '/user/' in post_info['url'] or '/u/' in post_info['url']:
            logger.debug(f"⚠️ Post promocional ignorado: {post_info['url']}")
            return None
        
        # FILTRO: Apenas aceita posts dos subreddits específicos
        allowed_subreddits = ['Overwatch', 'Overwatch_Memes']
        is_from_allowed = any(f'/r/{sub}/' in post_info['url'] for sub in allowed_subreddits)
        
        if not is_from_allowed:
            logger.debug(f"⚠️ Post de outro subreddit ignorado: {post_info['url']}")
            return None
    
    # Validação: precisa ter pelo menos ID e título
    if not post_info['id'] or not post_info['title']:
        return None
    
    # Debug: Log de mídia extraída
    if post_info['s_img']:
        logger.debug(f"Post {post_info['id']}: Imagem única encontrada")
    if post_info['m_img']:
        logger.debug(f"Post {post_info['id']}: Galeria com {len(post_info['m_img'])} imagens")
    if post_info['video']:
        logger.debug(f"Post {post_info['id']}: Vídeo encontrado")
    
    return post_info

def fetch_posts_from_reddit_html(subreddit, limit=50):
    """
    Busca posts do Reddit usando HTML scraping com sessão persistente.
    Retorna lista de posts ou None se falhar.
    """
    # Cria sessão para manter cookies
    session = requests.Session()
    
    # Headers mais realistas e completos
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cache-Control': 'max-age=0',
    })
    
    # Configura proxy (se disponível)
    if PROXY_HOST:
        if PROXY_USER and PROXY_PASS:
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
        
        session.proxies.update({
            "http": proxy_url,
            "https": proxy_url
        })
        logger.info(f"🔐 Usando proxy: {PROXY_HOST}:{PROXY_PORT}")
    else:
        logger.info("🔓 Conexão direta (sem proxy)")
    
    # ESTRATÉGIA: Primeiro acessa página principal para obter cookies
    try:
        logger.info("🍪 Obtendo cookies da página principal...")
        home_response = session.get("https://old.reddit.com/", timeout=10)
        if home_response.status_code == 200:
            logger.info(f"   ✅ Cookies obtidos: {len(session.cookies)} cookies")
        else:
            logger.warning(f"   ⚠️ Falha ao obter cookies: Status {home_response.status_code}")
    except Exception as e:
        logger.warning(f"   ⚠️ Erro ao obter cookies: {e}")
    
    # URLs para tentar (preferência para old.reddit)
    urls = [
        f"https://old.reddit.com/r/{subreddit}/",
        f"https://old.reddit.com/r/{subreddit}/hot/",
        f"https://www.reddit.com/r/{subreddit}/",
        f"https://www.reddit.com/r/{subreddit}/hot/",
    ]
    
    for url_index, url in enumerate(urls, 1):
        try:
            logger.info(f"🌐 Tentativa {url_index}/{len(urls)}: {url}")
            
            response = session.get(
                url,
                timeout=30,
                allow_redirects=True
            )
            
            logger.info(f"   Status: {response.status_code}")
            
            if response.status_code == 403:
                logger.warning(f"   ❌ 403 Forbidden - tentando próxima URL...")
                continue
            
            response.raise_for_status()
            
            # Parse HTML
            logger.info(f"   📄 HTML recebido: {len(response.text)} bytes")
            posts = parse_reddit_html(response.text)
            
            if not posts:
                logger.warning(f"   ⚠️ Nenhum post extraído do HTML")
                continue
            
            # Limita a quantidade de posts
            posts = posts[:limit]
            
            logger.info(f"✅ {len(posts)} posts extraídos de r/{subreddit}!")
            
            # Debug: mostra alguns posts
            for i, post in enumerate(posts[:3]):
                logger.info(f"   Post {i+1}: {post['title'][:50]}... (ID: {post['id']})")
            
            return posts
            
        except requests.exceptions.HTTPError as e:
            logger.warning(f"❌ HTTP {e.response.status_code} na tentativa {url_index}")
            continue
        except Exception as e:
            logger.warning(f"❌ Falha na tentativa {url_index}: {e}")
            continue
    
    logger.error("❌ Todas as tentativas falharam via CCProxy com HTML scraping")
    return None

def extractContent():
    """
    Sistema otimizado de extração com HTML scraping:
    
    NOVA LÓGICA:
    1. Se CCProxy ONLINE: SEMPRE busca novo JSON e substitui o mais antigo (FIFO)
    2. Se CCProxy OFFLINE: Consome batches salvos (do mais antigo pro mais novo)
    3. Retorna sempre 1 post novo
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
    
    # ✅ NOVA LÓGICA: Se proxy ONLINE, SEMPRE busca novo batch
    if proxy_available:
        logger.info("🟢 MODO ONLINE: Buscando NOVO batch via CCProxy (HTML SCRAPING)...")
        logger.info("   Estratégia: JSON fresco → Posts atualizados")
        
        # Escolhe subreddit aleatório
        subreddits = ['Overwatch', 'Overwatch_Memes']
        subreddit = choice(subreddits)
        
        logger.info(f"🎲 Subreddit selecionado: r/{subreddit}")
        
        # Busca posts via HTML scraping
        posts = fetch_posts_from_reddit_html(subreddit, limit=50)
        
        if posts:
            # Adiciona como 1 batch (FIFO automático: remove o mais antigo se já tiver 2)
            batch_id = add_json_batch(posts, subreddit)
            logger.info(f"💾 Batch #{batch_id} salvo com {len(posts)} posts")
            
            # Atualiza estatísticas
            stats = get_queue_stats()
            logger.info(f"📊 Fila atualizada: {stats['batches_count']} batch(es), {stats['available_posts']} posts disponíveis")
        else:
            logger.warning("⚠️ Falha ao buscar novos posts via HTML scraping")
            logger.info("   Usando batches salvos como fallback...")
    else:
        # Proxy OFFLINE: usa batches salvos
        logger.info("🔴 MODO OFFLINE: CCProxy indisponível, usando batches salvos")
        logger.info("   Estratégia: Consumir fila existente")
    
    # Busca próximo post não visto (varre todos os batches)
    logger.info("🔍 Procurando próximo post não visto...")
    batch_id, post = get_next_unposted_post()
    
    if post:
        logger.info(f"✨ Post encontrado no batch #{batch_id}")
        logger.info(f"📤 Título: {post['title'][:70]}...")
        return [post]
    else:
        logger.error("❌ FILA VAZIA! Nenhum post novo disponível.")
        if proxy_available:
            logger.error("   Isso é estranho - acabamos de buscar posts mas não achamos nenhum novo!")
        else:
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

if __name__ == "__main__":
    # Teste local
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("Sistema Otimizado com HTML Scraping do Reddit")
    print(f"Máximo de batches: {MAX_JSON_BATCHES}")
    print("=" * 60)
    print()
    
    posts = extractContent()
    debug_data(posts)