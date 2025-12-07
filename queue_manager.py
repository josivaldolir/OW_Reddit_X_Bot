import sqlite3
import json
import logging
from contextlib import closing
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = "seen_posts.db"
MAX_JSON_BATCHES = 2  # Máximo de JSONs armazenados

def initialize_queue_db():
    """Inicializa tabelas para sistema de fila otimizado"""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        # Tabela de batches de JSON (máximo 2)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS json_batches (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                subreddit TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posts_json TEXT,
                total_posts INTEGER,
                remaining_posts INTEGER
            )
        """)
        
        # Tabela de posts já vistos (permanente)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_posts (
                post_id TEXT PRIMARY KEY,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabela de pending (retry)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_posts (
                post_id TEXT PRIMARY KEY,
                content TEXT,
                img_paths TEXT,
                video_path TEXT,
                attempts INTEGER DEFAULT 0,
                last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("✓ Banco de dados inicializado")

def add_json_batch(posts, subreddit):
    """
    Adiciona um novo batch de posts (JSON completo).
    Se já existem 2 batches, remove o mais antigo (FIFO).
    Retorna batch_id criado.
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        # Conta quantos batches existem
        cursor = conn.execute("SELECT COUNT(*) FROM json_batches")
        count = cursor.fetchone()[0]
        
        # Se já tem 2 ou mais, remove o mais antigo
        if count >= MAX_JSON_BATCHES:
            cursor = conn.execute("""
                SELECT batch_id FROM json_batches 
                ORDER BY fetched_at ASC 
                LIMIT ?
            """, (count - MAX_JSON_BATCHES + 1,))
            
            old_batches = [row[0] for row in cursor.fetchall()]
            
            for old_id in old_batches:
                conn.execute("DELETE FROM json_batches WHERE batch_id = ?", (old_id,))
                logger.info(f"🗑️ Batch antigo #{old_id} removido (FIFO - mantendo apenas {MAX_JSON_BATCHES})")
        
        # Serializa posts para JSON
        posts_json = json.dumps(posts)
        total = len(posts)
        
        # Insere novo batch
        cursor = conn.execute("""
            INSERT INTO json_batches (subreddit, posts_json, total_posts, remaining_posts)
            VALUES (?, ?, ?, ?)
        """, (subreddit, posts_json, total, total))
        
        batch_id = cursor.lastrowid
        conn.commit()
        
        logger.info(f"✅ Novo batch #{batch_id} adicionado: {total} posts de r/{subreddit}")
        
        return batch_id

def get_next_unposted_post():
    """
    Varre TODOS os batches disponíveis procurando um post não visto.
    Retorna (batch_id, post) ou (None, None) se não houver posts novos.
    
    Estratégia:
    1. Pega batch mais ANTIGO primeiro (FIFO)
    2. Varre todos os posts do batch
    3. Encontra primeiro não visto
    4. Se batch esgotou, remove ele
    5. Passa para próximo batch
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        
        # Busca batches do mais ANTIGO para o mais NOVO (FIFO)
        cursor = conn.execute("""
            SELECT batch_id, subreddit, posts_json, total_posts, remaining_posts, fetched_at
            FROM json_batches
            ORDER BY fetched_at ASC
        """)
        
        batches = cursor.fetchall()
        
        if not batches:
            logger.warning("📭 Nenhum JSON disponível no banco de dados")
            return None, None
        
        logger.info(f"🔍 Verificando {len(batches)} batch(es) disponível(is)...")
        
        # Varre cada batch
        for batch in batches:
            batch_id = batch['batch_id']
            subreddit = batch['subreddit']
            posts = json.loads(batch['posts_json'])
            total = batch['total_posts']
            remaining = batch['remaining_posts']
            
            logger.info(f"📂 Batch #{batch_id} (r/{subreddit}): {remaining}/{total} posts restantes")
            
            # Varre TODOS os posts deste batch
            found_new = False
            for post in posts:
                post_id = post['id']
                
                # Verifica se já foi visto
                cursor = conn.execute(
                    "SELECT 1 FROM seen_posts WHERE post_id = ?", 
                    (post_id,)
                )
                
                if cursor.fetchone() is None:
                    # Post NOVO encontrado!
                    logger.info(f"✨ Post novo encontrado no batch #{batch_id}: {post.get('title', 'N/A')[:50]}...")
                    
                    # Atualiza remaining_posts
                    new_remaining = remaining - 1
                    conn.execute("""
                        UPDATE json_batches 
                        SET remaining_posts = ? 
                        WHERE batch_id = ?
                    """, (new_remaining, batch_id))
                    conn.commit()
                    
                    # Se batch esgotou, remove
                    if new_remaining <= 0:
                        conn.execute("DELETE FROM json_batches WHERE batch_id = ?", (batch_id,))
                        conn.commit()
                        logger.info(f"🗑️ Batch #{batch_id} esgotado e removido do banco")
                    
                    return batch_id, post
            
            # Se chegou aqui, todos os posts deste batch já foram vistos
            logger.warning(f"⚠️ Batch #{batch_id} não tem posts novos (todos já postados)")
            
            # Remove batch sem posts novos
            conn.execute("DELETE FROM json_batches WHERE batch_id = ?", (batch_id,))
            conn.commit()
            logger.info(f"🗑️ Batch #{batch_id} removido (sem posts novos)")
        
        # Se chegou aqui, todos os batches foram verificados e nenhum tem post novo
        logger.error("❌ Nenhum post novo encontrado em NENHUM batch!")
        logger.error("   Todos os posts disponíveis já foram postados.")
        return None, None

def get_queue_stats():
    """Retorna estatísticas detalhadas da fila"""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        # Total de batches
        cursor = conn.execute("SELECT COUNT(*) FROM json_batches")
        total_batches = cursor.fetchone()[0]
        
        # Total de posts disponíveis (remaining)
        cursor = conn.execute("SELECT SUM(remaining_posts) FROM json_batches")
        result = cursor.fetchone()[0]
        total_available = result if result else 0
        
        # Total de posts já postados
        cursor = conn.execute("SELECT COUNT(*) FROM seen_posts")
        total_posted = cursor.fetchone()[0]
        
        # Detalhes de cada batch
        cursor = conn.execute("""
            SELECT batch_id, subreddit, total_posts, remaining_posts, fetched_at
            FROM json_batches
            ORDER BY fetched_at ASC
        """)
        
        batches_detail = []
        for row in cursor.fetchall():
            batches_detail.append({
                'batch_id': row[0],
                'subreddit': row[1],
                'total': row[2],
                'remaining': row[3],
                'age': row[4]
            })
        
        return {
            'batches_count': total_batches,
            'available_posts': total_available,
            'posted_total': total_posted,
            'batches': batches_detail
        }

def is_post_seen(post_id):
    """Verifica se post já foi visto"""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cursor = conn.execute("SELECT 1 FROM seen_posts WHERE post_id = ?", (post_id,))
        return cursor.fetchone() is not None

def mark_post_as_seen(post_id):
    """Marca post como visto"""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen_posts (post_id) VALUES (?)", 
            (post_id,)
        )
        conn.commit()

def clear_all_batches():
    """Remove todos os batches (útil para reset/debug)"""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM json_batches")
        conn.commit()
        logger.info("🗑️ Todos os batches foram removidos")

if __name__ == "__main__":
    # Teste
    logging.basicConfig(level=logging.INFO)
    
    initialize_queue_db()
    
    print("\n📊 Estatísticas da fila:")
    stats = get_queue_stats()
    print(f"  Batches: {stats['batches_count']}")
    print(f"  Posts disponíveis: {stats['available_posts']}")
    print(f"  Posts postados: {stats['posted_total']}")
    
    if stats['batches']:
        print("\n📂 Detalhes dos batches:")
        for batch in stats['batches']:
            print(f"  #{batch['batch_id']}: r/{batch['subreddit']} - {batch['remaining']}/{batch['total']} restantes")