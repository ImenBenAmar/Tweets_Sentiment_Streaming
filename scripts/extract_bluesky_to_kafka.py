import asyncio
import json
import websockets
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_TOPIC = 'twitterdata'
SEARCH_KEYWORD = "NBA" 
# ---------------------

producer = KafkaProducer(bootstrap_servers='localhost:9092')

async def listen_to_jetstream():
    # On se connecte à Jetstream (plus léger que le Firehose)
    # on demande seulement les "posts" (wantedCollections)
    uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
    
    print(f"Connecting to Jetstream... Filtering for '{SEARCH_KEYWORD}' in ENGLISH only.")

    async with websockets.connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)

                # Jetstream envoie directement du JSON
                # On vérifie si c'est une création de post
                if data['kind'] == 'commit' and data['commit']['operation'] == 'create':
                    record = data['commit']['record']
                    
                    # On récupère le texte
                    text_content = record.get('text', '')
                    
                    # On récupère la liste des langues (ex: ['en'], ['fr'], ou [])
                    # On utilise .get() avec une liste vide par défaut pour éviter les erreurs
                    langs = record.get('langs', [])

                    # --- FILTRES ---
                    # 1. Le mot clé doit être présent (ex: NBA)
                    # 2. 'en' (English) doit être dans la liste des langues déclarées
                    if SEARCH_KEYWORD.lower() in text_content.lower() and 'en' in langs:
                        
                        payload = {
                            "text": text_content,
                            "created_at": record.get('createdAt')
                        }
                        
                        # Envoi à Kafka
                        producer.send(KAFKA_TOPIC, json.dumps(payload).encode('utf-8'))
                        
                        # On affiche un petit point pour dire que ça marche
                        # On ajoute [EN] dans le print pour confirmer visuellement
                        print(f"[EN] Sent: {text_content[:40]}...")

            except websockets.exceptions.ConnectionClosed:
                print("Connection closed. Reconnecting...")
                break
            except Exception as e:
                # Ignorer les erreurs de parsing mineures
                pass

if __name__ == '__main__':
    while True:
        try:
            asyncio.run(listen_to_jetstream())
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}. Retrying in 3s...")
            import time
            time.sleep(3)