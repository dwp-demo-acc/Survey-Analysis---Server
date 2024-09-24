import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# # Initialize a connection to the blob storage
# blob_service_client = BlobServiceClient.from_connection_string('your_connection_string')

# # Name of the container where the nltk_data is stored
# container_name = 'nltk-data'

# # Create a blob client using the blob storage client and specify the container name
# container_client = blob_service_client.get_container_client(container_name)

# def download_nltk_data_from_blob_storage(local_path='files/nltk_data'):
#     # List all blobs in the container and download them
#     blob_list = container_client.list_blobs()
#     for blob in blob_list:
#         blob_client = container_client.get_blob_client(blob)
#         download_file_path = os.path.join(local_path, blob.name)
#         os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
#         with open(download_file_path, "wb") as download_file:
#             download_file.write(blob_client.download_blob().readall())

download_location = 'files/nltk_data'
# Call the download function before using the SentimentIntensityAnalyzer
# download_nltk_data_from_blob_storage(download_location)

# # Download NLTK data to the specified location
# nltk.download('all', download_dir=download_location)
# nltk.data.path.append(download_location)

# Set the location where the NLTK data is expected to be found in the deployment environment
# download_location = os.path.join(os.path.dirname(__file__), 'nltk_data')
# print('download_location: ', download_location)
nltk.data.path.append(download_location)

analyzer = SentimentIntensityAnalyzer()

class SentimentAnalyzer:

    def __init__(self, data_frame) -> None:
        self.df = data_frame

    def _preprocess_text(self, text):
        text = str(text)

        # Tokenize the text
        tokens = word_tokenize(text.lower())

        # Remove stop words
        filtered_tokens = [token for token in tokens if token not in stopwords.words('english')]

        # Lemmatize the tokens
        lemmatizer = WordNetLemmatizer()
        lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

        # Join the tokens back into a string
        processed_text = ' '.join(lemmatized_tokens)

        return processed_text

    def _get_sentiment(self, text):
        scores = analyzer.polarity_scores(text)

        sentiment = 'Positive' if scores['pos'] > 0 else 'Negative'

        return sentiment

    def update_local_df_with_sentiment(self, column_name: str):
        self.df[column_name] = self.df[column_name].apply(self._preprocess_text)
        new_column_name = 'sentiment_' + column_name.replace(' ', '_')
        self.df.insert(loc=self.df.columns.get_loc(column_name) + 1, column=new_column_name,
                       value=self.df[column_name].apply(self._get_sentiment))
        return self.df, new_column_name

# Dev section
# self.df = pd.read_excel("files/Survey CS Project Feedback Sentiment Analysis.xlsx")
# sentiment = SentimentAnalyzer()

# 'Additional comments' as example
# df = sentiment.update_local_df_with_sentiment("Additional comments")

# source : https://www.datacamp.com/tutorial/text-analytics-beginners-nltk
