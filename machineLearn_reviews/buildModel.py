import loadData
import mlpModel

def main():
    trainTest = loadData.load_imdb_sentiment_analysis_dataset('/Users/ting/Doc/ComputerS/MachLearn/IMDBmovie')
    mlpModel.train_ngram_model(trainTest)

if __name__ == '__main__':
    main()

