import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn import svm
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split as tts
from sklearn.metrics import accuracy_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import f1_score
import time

data_xtest = pd.read_csv("./lab1_dataset/X_test.csv")
data_ytest = pd.read_csv("./lab1_dataset/Y_test.csv")
data_xtrain = pd.read_csv("./lab1_dataset/X_train.csv")
data_ytrain = pd.read_csv("./lab1_dataset/Y_train.csv")

# data_xtest.head()

loss_all = []
loss_show = []
# ac_tr_time_all = []

# sklearn SVM
def sklearnSVM(x_test, y_test, x_train, y_train):
    predictor = svm.SVC(gamma='scale', C=0.7, decision_function_shape='ovr', kernel='linear')
    # 开始训练
    predictor.fit(x_train, y_train)
    y_test_predicted = predictor.predict(x_test)
    # print("sklearn SVM: y_test_predicted")
    # print(y_test_predicted)
    # 进行评估
    print("accuracy on test dataset: {}".format(accuracy_score(y_test, y_test_predicted)))
    print("recall on test dataset: {}".format(recall_score(y_test, y_test_predicted)))
    print("precision on test dataset: {}".format(precision_score(y_test, y_test_predicted)))



# 参数调整
def sklearnSVM_change(x_test, y_test, x_train, y_train):
    a = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    for i in range(len(a)):
        print("now the c is:", a[i])
        predictor = svm.SVC(gamma='scale', C=a[i], decision_function_shape='ovr', kernel='linear')
        # 开始训练
        predictor.fit(x_train, y_train)
        y_test_predicted = predictor.predict(x_test)
        # print("sklearn SVM: y_test_predicted")
        # print(y_test_predicted)
        # 进行评估
        print("accuracy on test dataset: {}".format(accuracy_score(y_test, y_test_predicted)))
        print("recall on test dataset: {}".format(recall_score(y_test, y_test_predicted)))
        print("precision on test dataset: {}".format(precision_score(y_test, y_test_predicted)))
        


# SVM model using gradient descent method
class SVM:
    
    def init(self, learning_rate=0.00075, lambda_param=0.01, n_iters=1000):
        self.lr = learning_rate # 𝛼 in formula
        self.lambda_param = lambda_param
        self.n_iters = n_iters 
        self.w = None
        self.b = None

    def fit(self, X, y):
        n_samples, n_features = X.shape
        self.w = np.zeros(n_features)
        self.b = 0
        loss_show.clear()
        for _ in range(self.n_iters):
            #每个iter(五十个iter记一次吧)
            # print("now is iter get loss")
            # loss_all.append(get_all_loss())
            loss_all.clear()
            for idx, x_i in enumerate(X):
                self.update(x_i, y[idx])
            loss_show.append(sum(loss_all))
            loss_all.clear()
            # training_time_end = time.time()
            
        
    def update(self,x,y):
        y = y[0]
        if y == 0: 
            y = -1
        distance = 1 - (y * (np.dot(x, self.w) + self.b))
        hinge_loss = max(0,distance)
        loss_all.append(hinge_loss)
        # add_all_loss(hinge_loss)
        if(hinge_loss == 0):
            self.w = self.w - self.lr * (2 * self.lambda_param * self.w)
        else: 
            self.w = self.w - self.lr * (2 * self.lambda_param * self.w - np.dot(x,y))
            self.b = self.b + self.lr * y
        
        
    def predict(self, X):
        eq = np.dot(X, self.w) + self.b
        return np.sign(eq)
    
    
    


#参数调整
def gdmSVM_test(data_xtest, data_ytest, data_xtrain, data_ytrain):
    clf = SVM()
    a = [0.00005, 0.0001, 0.00015, 0.0002, 0.00025, 0.0003, 0.00035, 0.0004, 0.00045, 0.0005, 0.00055, 0.0006, 0.00065, 0.0007, 0.00075, 0.0008, 0.00085, 0.0009, 0.00095]
    b = [0.001, 0.0015, 0.002, 0.0025, 0.003, 0.0035, 0.004, 0.0045, 0.005, 0.0055, 0.006, 0.0065, 0.007, 0.0075, 0.008, 0.0085, 0.009, 0.0095, 0.01, 0.0105, 0.0110, 0.0115, 0.012, 0.0125, 0.013, 0.0135]
    for i in range(len(a)):
        print("now is +", a[i])
        clf.init(a[i])
        clf.fit(data_xtrain.to_numpy(), data_ytrain.to_numpy())
        y_test_predicted = clf.predict(data_xtest.to_numpy())
        # print(y_test_predicted)
        for i in range(len(y_test_predicted)):
            # print(len(y_test_predicted))
            if y_test_predicted[i] == -1:
                y_test_predicted[i] = 0
        print("accuracy on test dataset: {}".format(accuracy_score(data_ytest.to_numpy(), y_test_predicted)))
        print("recall on test dataset: {}".format(recall_score(data_ytest.to_numpy(), y_test_predicted)))
        print("precision on test dataset: {}".format(precision_score(data_ytest.to_numpy(), y_test_predicted)))
    for i in range(len(b)):
        print("now is +", b[i])
        clf.init(0.00075, b[i])
        clf.fit(data_xtrain.to_numpy(), data_ytrain.to_numpy())
        y_test_predicted = clf.predict(data_xtest.to_numpy())
        # print(y_test_predicted)
        for i in range(len(y_test_predicted)):
            # print(len(y_test_predicted))
            if y_test_predicted[i] == -1:
                y_test_predicted[i] = 0
        print("accuracy on test dataset: {}".format(accuracy_score(data_ytest.to_numpy(), y_test_predicted)))
        print("recall on test dataset: {}".format(recall_score(data_ytest.to_numpy(), y_test_predicted)))
        print("precision on test dataset: {}".format(precision_score(data_ytest.to_numpy(), y_test_predicted)))


#梯度下降的SVM
def gdmSVM(data_xtest, data_ytest, data_xtrain, data_ytrain):
    clf = SVM()
    clf.init()
    clf.fit(data_xtrain.to_numpy(), data_ytrain.to_numpy())
    y_test_predicted = clf.predict(data_xtest.to_numpy())
    for i in range(len(y_test_predicted)):
        # print(len(y_test_predicted))
        if y_test_predicted[i] == -1:
            y_test_predicted[i] = 0
    # print(y_test_predicted)
    print("accuracy on test dataset: {}".format(accuracy_score(data_ytest.to_numpy(), y_test_predicted)))
    print("recall on test dataset: {}".format(recall_score(data_ytest.to_numpy(), y_test_predicted)))
    print("precision on test dataset: {}".format(precision_score(data_ytest.to_numpy(), y_test_predicted)))
    
   
loss_show_every100 =[] 
def get_loss_show():
    #每五十个iter算一次吧
    print(len(loss_show))
    i = 0
    while(i < 1000):
        loss_show_every100.append(loss_show[i])
        i = i + 20
    print(loss_show_every100)   
    data1 = pd.DataFrame(loss_show_every100)
    data1.to_csv('loss.csv')     
    
   
#直接通过对iter的模拟来模拟accuracy
accuracy = [] 
def get_all_accuracy():
    clf = SVM()
    for i in range(1000):
        if i % 10 == 0:
            clf.init(0.00075, 0.0001, i)
            clf.fit(data_xtrain.to_numpy(), data_ytrain.to_numpy())
            y_test_predicted = clf.predict(data_xtest.to_numpy())
            for i in range(len(y_test_predicted)):
                # print(len(y_test_predicted))
                if y_test_predicted[i] == -1:
                    y_test_predicted[i] = 0
            # print(y_test_predicted)
            accuracy.append(format(accuracy_score(data_ytest.to_numpy(), y_test_predicted)))
    data1 = pd.DataFrame(accuracy)
    data1.to_csv('accuracy.csv')   


def final():
    #gdm
    start_time1 = time.time()
    print("now is gdmSVM")
    gdmSVM(data_xtest, data_ytest, data_xtrain, data_ytrain)
    end_time1 = time.time()
    print("耗时: {:.2f}秒".format(end_time1 - start_time1))
    #skl
    start_time1 = time.time()
    print("now is sklearn SVM")
    sklearnSVM(data_xtest.to_numpy(), data_ytest.to_numpy(), data_xtrain.to_numpy(), data_ytrain.to_numpy()) 
    end_time1 = time.time()
    print("耗时: {:.2f}秒".format(end_time1 - start_time1))   


    # in order to get loss and accuracy
    get_loss_show()
    # maybe a little slow
    get_all_accuracy()
    # print(loss_all)



final()
# sklearnSVM_change(data_xtest.to_numpy(), data_ytest.to_numpy(), data_xtrain.to_numpy(), data_ytrain.to_numpy())
# gdmSVM_test(data_xtest, data_ytest, data_xtrain, data_ytrain)
