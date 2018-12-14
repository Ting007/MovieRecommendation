import loadData
import tensorflow as tf
import NormData
from tensorflow.python.keras import models
from tensorflow.python.keras.layers import Dense
from tensorflow.python.keras.layers import Dropout
from tensorflow.python.keras.models import load_model

def get_num_classes(labels):
    num_classes = max(labels) + 1
    missing_classes = [i for i in range(num_classes) if i not in labels]
    if len(missing_classes):
        raise ValueError('Missing samples with label value(s) '
                         '{missing_classes}. Please make sure you have '
                         'at least one sample for every label value '
                         'in the range(0, {max_class})'.format(
                            missing_classes=missing_classes,
                            max_class=num_classes - 1))

    if num_classes <= 1:
        raise ValueError('Invalid number of labels: {num_classes}.'
                         'Please make sure there are at least two classes '
                         'of samples'.format(num_classes=num_classes))
    return num_classes

def use_ngram_model(data):
    (train_texts, train_labels), (val_texts, val_labels) = data

    # Vectorize texts.
    x_train, x_val = NormData.ngram_vectorize(
        train_texts, train_labels, val_texts)

    # Create model instance.
    model = load_model('IMDb_mlp_model.h5')
    history = model.predict_classes(x_val)
    print(x_val[0], history[0])
    # print("X=%s, Predicted=%s" % (i[0], i[1]))

    return

def main():
    trainTest = loadData.load_imdb_sentiment_analysis_dataset('/Users/ting/Doc/ComputerS/MachLearn/IMDBmovie')
    use_ngram_model(trainTest)

if __name__ == '__main__':
    main()