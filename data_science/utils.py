from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

eng_stopwords = list(set(stopwords.words('english')))

@udf(returnType=StringType())
def remove_stopwords(text: str, stopwords: list[str]) -> str:
    if text is None:
        return ""
    words = word_tokenize(text)
    words = [word for word in words if word not in stopwords]
    return " ".join(words)

@udf(returnType=IntegerType())
def count_special_char(text: str) -> int:
    if text is None:
        return 0
    count = 0
    for char in text:
        if not char.isalnum() and not char.isspace():
            count += 1
    return count

@udf(returnType=IntegerType())
def count_fake_keywords(text: str, title: str, keywords: list[str]) -> int:
    count = 0
    text_lower = text.lower() if text else ""
    title_lower = title.lower() if title else ""
    for keyword in keywords:
        if keyword in text_lower:
            count += 1
        if keyword in title_lower:
            count += 1
    return count