import logging
from enum import Enum
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

import numpy as np
import tensorflow as tf
from tensorflow.keras.preprocessing.sequence import pad_sequences
from huggingface_hub import hf_hub_download
import pickle

nltk.download('punkt_tab')
nltk.download('stopwords')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='app.log', mode="w"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger()

logging.getLogger('py4j.clientserver').setLevel(logging.ERROR)

class KafkaTopics(Enum):
    SCRAPED_DATA = "scraped-data"
    PROCESSED_DATA = "processed-data"
    LABELLED_DATA = "labelled-data"
    VERIFY_DATA = "verify-data"

eng_stopwords = list(set(stopwords.words('english')))

@udf(returnType=StringType())
def remove_stopwords(text: str, stopwords: list[str]) -> str:
    if text is None:
        return ""
    words = word_tokenize(text)
    words = [word for word in words if word not in stopwords]
    return " ".join(words)

repo_id = "dl-quad/fake-news-bi-lstm-dl-quadrilateral"
model_filename = "lstm/fake_news_bi_lstm_model.keras"        
tokenizer_filename = "lstm/tokenizer.pkl"
 
model_path = hf_hub_download(repo_id=repo_id, filename=model_filename)
tokenizer_path = hf_hub_download(repo_id=repo_id, filename=tokenizer_filename)

model = tf.keras.models.load_model(model_path)
with open(tokenizer_path, "rb") as f:
    tokenizer = pickle.load(f)

def infer_text_label(text, maxlen=5000):
    seq = tokenizer.texts_to_sequences([text])
    seq = pad_sequences(seq, maxlen=maxlen, padding='post')
   
    logit = model.predict(seq)[0][0]
    print(logit,"logit")
   
    prob = 1 / (1 + np.exp(-logit))
   
    label = "real" if prob >= 0.5 else "fake"
   
    return label, float(prob)