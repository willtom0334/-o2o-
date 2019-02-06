rm(list = ls())
###################################### 数据处理 ###################################################
require(ROCR)
require(caret)
require(sqldf)
require(Matrix)
require(xgboost)
require(glmnet)
require(snowfall)
require(nnet)
require(car)
require(gbm)
require(randomForest)
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.7.0_79\\jre')
require(snowfall)
sfInit( parallel=TRUE, cpus=2 )
memory.limit(102400)
st_range<-function(x){
  return((x-min(x))/(max(x)-min(x)))}
train <- read.csv("E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_fourth.csv")
train <- train[-1]
x = as.data.frame( apply(train[,c(1:72)],2,st_range) )
y = train[,73]
train = cbind(x,y)
revised <- read.csv("E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\revised_two_fourth.csv")
revised <- revised[-1]
x = as.data.frame( apply(revised[,c(1:72)],2,st_range) )
y = as.data.frame(revised[,c(73:76)])
revised = cbind(x,y)
# #--------------------------------------------------
train$stack_order <- seq(1,nrow(train))
train$y <- as.factor(train$y)
y_1 <- train[train$y==1,]
y_0 <- train[train$y==0,]
set.seed(2018)
folds_0 <- createFolds(y_0$y,k = 10)
str(folds_0)
folds_1 <- createFolds(y_1$y,k = 3)
str(folds_1)
#-------------- model 1  Logistic ------------
LR <- as.data.frame( train$stack_order)  #  20次交叉验证后产生的结果，以后做训练集
revised_LR <- as.data.frame( revised$revised_order)   #每次都取20次预测后的均值，做提交集
AUC <- data.frame()  #先训练个 最侍的神经元数量
n <- 1
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    x = as.matrix(trainning[,c(1:72)])
    y = as.matrix(trainning[,73])
    lasso.cv = cv.glmnet(x = x,y = y,family = "binomial",alpha = 1,nfold = 10)
    lasso.min = glmnet(x = x,y = y,family = "binomial",alpha = 1,lambda = lasso.cv$lambda.min)
    
    testing = rbind(y_1[i,],y_0[-j,])
    test_x = as.matrix(testing[,c(1:72)]) 
    testing$LR_pred = predict(lasso.min,newx = test_x)
    testing$LR_p = 1/(1 + exp(-testing$LR_pred))
    testing$out = 1
    testing$out[testing$LR_p < 0.5] = 0
    pred_test = prediction(testing$out,testing$y)
    perf_test = performance(pred_test,"tpr","fpr")
    test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
    print(c('test_AUC:',test_auc))
    AUC[n,1] = n
    AUC[n,2] = test_auc
    n = n + 1
    testing = train
    test_x = as.matrix(testing[,c(1:72)]) 
    testing$LR_pred = predict(lasso.min,newx = test_x)
    testing$LR_p = 1/(1 + exp(-testing$LR_pred))
    testing = subset(testing,select = c('stack_order','LR_p'))
    LR = merge(LR,testing,by.x = "train$stack_order",by.y = 'stack_order',all.x = T)
    revised_x = as.matrix(revised[,c(1:72)])
    revised_pred = 1 / (1 + exp(-predict(lasso.min,newx = revised_x)))
    revised_pred = as.data.frame(revised_pred)
    revised_LR = cbind(revised_LR,revised_pred)
  }
}

LR$count <- 0
LR$sum_0 <- 0
LR$sum_1 <- 0
for(i in 2:31){
  LR$is = ifelse( (LR[,i] >=0.5 & is.na(LR[,i]) != T) == T ,1,0)
  LR$is[is.na(LR[,i])] = 2
  LR$count =  LR$is +LR$count
  LR$sum_0[LR$is==0]  = LR[LR$is==0,i] + LR$sum_0[LR$is==0] 
  LR$sum_1[LR$is==1]  = LR[LR$is==1,i] + LR$sum_1[LR$is==1]
}
LR$LR_P <- LR$sum_1/LR$count
LR$LR_P[LR$count != 30] <- LR$sum_0[LR$count != 30]/(30-LR$count[LR$count != 30])
LR$LR_out <- ifelse(LR$LR_P >0.5,1,0)
pred_test = prediction(LR$LR_out,train$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))
mean(AUC$V2)
#---------------------------
revised_LR$count <- 0
revised_LR$sum_0 <- 0
revised_LR$sum_1 <- 0
for(i in 2:31){
  revised_LR$is = ifelse((revised_LR[,i] >=0.5) == T,1,0)
  revised_LR$count =  revised_LR$is +revised_LR$count
  revised_LR$sum_0[revised_LR$is==0]  = revised_LR[revised_LR$is==0,i] + revised_LR$sum_0[revised_LR$is==0] 
  revised_LR$sum_1[revised_LR$is==1]  = revised_LR[revised_LR$is==1,i] + revised_LR$sum_1[revised_LR$is==1]
}
revised_LR$LR_P <- revised_LR$sum_1/revised_LR$count
revised_LR$LR_P[revised_LR$count != 30] <- revised_LR$sum_0[revised_LR$count != 30]/
  (30-revised_LR$count[revised_LR$count != 30])
revised_LR$out <-  ifelse(revised_LR$LR_P >0.5,1,0)
table(revised_LR$out)
write.csv(revised_LR,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_LR_1.csv')
write.csv(LR,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\LR_1.csv')
rm(lasso.min,lasso.cv,revised_pred,test_x,revised_x,x,i,j,st_range,y)

#-------------- model 2  BP ------------
NNET <- as.data.frame( train$stack_order) 
revised_NNET <- as.data.frame( revised$revised_order) 
auc_nnet <- data.frame()
n <- 1
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    size <- data.frame()  
    m <- 1
    train_midd <- as.data.frame( train$stack_order) 
    revised_midd <- as.data.frame( revised$revised_order) 
    for (k in seq(from=6,to=8,by=1)){
      testing = rbind(y_1[i,],y_0[-j,])
      model_nnet <- nnet(y ~ .,linout = F,size = k,decay = 0.01,
                         maxit = 10000,data = trainning[,c(1:(length(trainning)-1))])
      testing$pred = predict(model_nnet,testing)
      testing$out = 1
      testing$out[testing$pred < 0.5] = 0
      pred_test = prediction(testing$out,testing$y)
      perf_test = performance(pred_test,"tpr","fpr")
      test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
      print(test_auc)
      size[m,1] = k
      size[m,2] = test_auc
      m = m + 1
      testing = train
      testing$pred = predict(model_nnet,testing)
      testing = subset(testing,select = c('stack_order','pred'))
      train_midd = merge(train_midd,testing,by.x = "train$stack_order",by.y = 'stack_order',all.x = T)
      revised_pred = predict( model_nnet,revised) 
      revised_pred = as.data.frame(revised_pred)
      revised_midd = cbind(revised_midd,revised_pred)
    }
    names(size) <- c('size','test_auc')
    which.max(size$test_auc)
    auc_nnet[n,1] = n
    auc_nnet[n,2] = size[which.max(size$test_auc),2]
    print(c('现在完了第',n,'个'))
    n = n + 1
    
    testing = train_midd[,c(1, (size[which.max(size$test_auc),1]-4))]
    NNET = merge(NNET,testing,by = "train$stack_order",all.x = T)
    
    revised_pred = revised_midd[,(size[which.max(size$test_auc),1]-4)]
    revised_pred = as.data.frame(revised_pred)
    revised_NNET = cbind(revised_NNET,revised_pred)
  }
}
NNET$count <- 0
NNET$sum_0 <- 0
NNET$sum_1 <- 0
for(i in 2:31){
  NNET$is = ifelse( (NNET[,i] >=0.5 & is.na(NNET[,i]) != T) == T ,1,0)
  NNET$is[is.na(NNET[,i])] = 2
  NNET$count =  NNET$is +NNET$count
  NNET$sum_0[NNET$is==0]  = NNET[NNET$is==0,i] + NNET$sum_0[NNET$is==0] 
  NNET$sum_1[NNET$is==1]  = NNET[NNET$is==1,i] + NNET$sum_1[NNET$is==1]
}

NNET$NNET_P <- NNET$sum_1/NNET$count
NNET$NNET_P[NNET$count <= 18] <- NNET$sum_0[NNET$count <= 18]/(30 - NNET$count[NNET$count <= 18])
NNET$NNET_out <- ifelse(NNET$NNET_P >0.5,1,0)
pred_test = prediction(NNET$NNET_out,train$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))
mean(auc_nnet$V2)
table(NNET$NNET_out)
#---------------------------
revised_NNET$count <- 0
revised_NNET$sum_0 <- 0
revised_NNET$sum_1 <- 0
for(i in 2:31){
  revised_NNET$is = ifelse((revised_NNET[,i] >=0.5) == T,1,0)
  revised_NNET$count =  revised_NNET$is +revised_NNET$count
  revised_NNET$sum_0[revised_NNET$is==0]  = revised_NNET[revised_NNET$is==0,i] + revised_NNET$sum_0[revised_NNET$is==0] 
  revised_NNET$sum_1[revised_NNET$is==1]  = revised_NNET[revised_NNET$is==1,i] + revised_NNET$sum_1[revised_NNET$is==1]
}
revised_NNET$NNET_P <- revised_NNET$sum_1/revised_NNET$count
revised_NNET$NNET_P[revised_NNET$count <= 18] <- revised_NNET$sum_0[revised_NNET$count <= 18]/
  (30-revised_NNET$count[revised_NNET$count <= 18])
revised_NNET$NNET_out <-  ifelse(revised_NNET$NNET_P >0.5,1,0)
table(revised_NNET$NNET_out)
rm(model_nnet)
write.csv(revised_NNET,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_NNET_2.csv')
write.csv(NNET,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\NNET_2.csv')


tijiao <- cbind(revised[73:76],revised_NNET[,c(1,36)]) 
tijiao <- tijiao[-4]
tijiao <- tijiao[order(tijiao$`revised$revised_order`),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
require(sqldf)
tijiao <- sqldf("select User_id,Coupon_id,ymd,NNET_P from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')
#-----------------  gbm   ------------
GBDT <- as.data.frame( train$stack_order) 
auc_GDBT <- data.frame()
revised_GBDT <- as.data.frame( revised$revised_order) 
y_1 <- train[train$y==1,]
y_0 <- train[train$y==0,]
y_1$y <- as.numeric(as.character(y_1$y))
y_0$y <- as.numeric(as.character(y_0$y)) 
n <- 1
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    size <- data.frame()  
    m <- 1
    train_midd <- as.data.frame( train$stack_order) 
    revised_midd <- as.data.frame( revised$revised_order) 
    for (k in seq(from=7,to=10,by=1)){
      testing = rbind(y_1[i,],y_0[-j,])
      gbdt_model = gbm(y ~ .,distribution = "bernoulli",data = trainning[,c(1:(length(trainning)-1))],
                       n.trees = 1200,shrinkage = 0.01,verbose = T,
                       interaction.depth = k,n.minobsinnode = 20)
      best.iter = gbm.perf(gbdt_model)
      print(best.iter)
      testing$pred = predict(gbdt_model,testing,best.iter,type = 'response')
      testing$out = 1
      testing$out[testing$pred < 0.5] = 0
      pred_test = prediction(testing$out,testing$y)
      perf_test = performance(pred_test,"tpr","fpr")
      test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
      print(test_auc)
      
      size[m,1] = k
      size[m,2] = test_auc
      m = m + 1
      
      testing = train
      testing$pred = predict(gbdt_model,testing,best.iter,type = 'response')
      testing = subset(testing,select = c('stack_order','pred'))
      train_midd = merge(train_midd,testing,by.x = "train$stack_order",by.y = 'stack_order',all.x = T)
      
      revised_pred = predict(gbdt_model,revised,best.iter,type = 'response')
      revised_pred = as.data.frame(revised_pred)
      revised_midd = cbind(revised_midd,revised_pred)
    }
    names(size) <- c('mtry','test_auc')
    which.max(size$test_auc)
    
    print(c('test_AUC:',test_auc,'第',n,'个'))
    auc_GDBT[n,1] = n
    auc_GDBT[n,2] = test_auc
    n = n + 1
    
    testing = train_midd[,c(1, (size[which.max(size$test_auc),1]-5))]
    GBDT= merge(GBDT,testing,by = "train$stack_order",all.x = T)
    
    revised_pred = revised_midd[,(size[which.max(size$test_auc),1]-5)]
    revised_pred = as.data.frame(revised_pred)
    revised_GBDT = cbind(revised_GBDT,revised_pred)
  }
}



GBDT$count <- 0
GBDT$sum_0 <- 0
GBDT$sum_1 <- 0
for(i in 2:31){
  GBDT$is = ifelse( (GBDT[,i] >=0.5 & is.na(GBDT[,i]) != T) == T ,1,0)
  GBDT$is[is.na(GBDT[,i])] = 2
  GBDT$count =  GBDT$is +GBDT$count
  GBDT$sum_0[GBDT$is==0]  = GBDT[GBDT$is==0,i] + GBDT$sum_0[GBDT$is==0] 
  GBDT$sum_1[GBDT$is==1]  = GBDT[GBDT$is==1,i] + GBDT$sum_1[GBDT$is==1]
}
GBDT$GBDT_P <- GBDT$sum_1/GBDT$count
GBDT$GBDT_P[GBDT$count <= 21] <- GBDT$sum_0[GBDT$count <= 21]/
  (30 - GBDT$count[GBDT$count <= 21])
GBDT$GBDT_out <- ifelse(GBDT$GBDT_P >0.5,1,0)
pred_test = prediction(GBDT$GBDT_out,train$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))
mean(auc_GDBT$V2)
table(GBDT$GBDT_out)
#---------------------------
revised_GBDT$count <- 0
revised_GBDT$sum_0 <- 0
revised_GBDT$sum_1 <- 0
for(i in 2:31){
  revised_GBDT$is = ifelse((revised_GBDT[,i] >=0.5) == T,1,0)
  revised_GBDT$count =  revised_GBDT$is +revised_GBDT$count
  revised_GBDT$sum_0[revised_GBDT$is==0]  = revised_GBDT[revised_GBDT$is==0,i] + revised_GBDT$sum_0[revised_GBDT$is==0] 
  revised_GBDT$sum_1[revised_GBDT$is==1]  = revised_GBDT[revised_GBDT$is==1,i] + revised_GBDT$sum_1[revised_GBDT$is==1]
}
revised_GBDT$GBDT_P <- revised_GBDT$sum_1/revised_GBDT$count
revised_GBDT$GBDT_P[revised_GBDT$count <= 21] <- revised_GBDT$sum_0[revised_GBDT$count <= 21]/
  (30-revised_GBDT$count[revised_GBDT$count <= 21])
revised_GBDT$GBDT_out <-  ifelse(revised_GBDT$GBDT_P >0.5,1,0)
table(revised_GBDT$GBDT_out)
rm(gbdt_model)
write.csv(revised_GBDT,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_GBDT_1.csv')
write.csv(GBDT,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\GBDT_1.csv')  



tijiao <- cbind(revised[73:76],revised_GBDT[,c(1,36)]) 
tijiao <- tijiao[-4]
tijiao <- tijiao[order(tijiao$`revised$revised_order`),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
tijiao <- sqldf("select User_id,Coupon_id,ymd,GBDT_P from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')  
rm(folds_0,folds_1,perf_test,pred_test,revised_midd)
rm(revised_pred,size,testing,train_midd,trainning,y_0,y_1)
rm(best.iter,i,j,k,m,n,test_auc)
################################################
RF <- as.data.frame( train$stack_order)
revised_RF <- as.data.frame( revised$revised_order)
auc_rf <- data.frame()
n <- 1
y_1$y <- as.factor(y_1$y)
y_0$y <- as.factor(y_0$y)
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    mtry <- data.frame()
    m <- 1
    train_midd <- as.data.frame( train$stack_order)
    revised_midd <- as.data.frame( revised$revised_order)
    for (k in seq(from=2,to=7,by=1)){
      testing = rbind(y_1[i,],y_0[-j,])
      rf_model <- randomForest(y ~ .,data = trainning[,c(1:73)],mtry = k,ntree = 550)
      testing$pred = predict(rf_model,testing[1:72],type = 'prob')[,2]
      testing$out = 1
      testing$out[testing$pred < 0.5] = 0
      pred_test = prediction(testing$out,testing$y)
      perf_test = performance(pred_test,"tpr","fpr")
      test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
      print(c(k,test_auc))
      mtry[m,1] = k
      mtry[m,2] = test_auc
      m = m + 1
      
      testing = train
      testing$pred = predict(rf_model,testing[1:72],type = 'prob')[,2]
      testing = subset(testing,select = c('stack_order','pred'))
      train_midd = merge(train_midd,testing,by.x = "train$stack_order",by.y = 'stack_order',all.x = T)

      revised_pred = predict( rf_model,revised[,c(1:72)],type = 'prob')[,2]
      revised_pred = as.data.frame(revised_pred)
      revised_midd = cbind(revised_midd,revised_pred)
    }
    names(mtry) <- c('mtry','test_auc')
    which.max(mtry$test_auc)


    auc_rf[n,1] = n
    auc_rf[n,2] = mtry[which.max(mtry$test_auc),2]
    print(c('现在完了第',n,'个'))
    n = n + 1

    testing = train_midd[,c(1, (mtry[which.max(mtry$test_auc),1]))]
    RF = merge(RF,testing,by = "train$stack_order",all.x = T)

    revised_pred = revised_midd[,(mtry[which.max(mtry$test_auc),1])]
    revised_pred = as.data.frame(revised_pred)
    revised_RF = cbind(revised_RF,revised_pred)
  }
}

RF$count <- 0
RF$sum_0 <- 0
RF$sum_1 <- 0
for(i in 2:31){
  RF$is = ifelse( (RF[,i] >=0.5 & is.na(RF[,i]) != T) == T ,1,0)
  RF$is[is.na(RF[,i])] = 2
  RF$count =  RF$is +RF$count
  RF$sum_0[RF$is==0]  = RF[RF$is==0,i] + RF$sum_0[RF$is==0] 
  RF$sum_1[RF$is==1]  = RF[RF$is==1,i] + RF$sum_1[RF$is==1]
}
RF$RF_P <- RF$sum_1/RF$count
RF$RF_P[RF$count <= 27] <- RF$sum_0[RF$count <= 27]/
  (30 - RF$count[RF$count <= 27])
RF$RF_out <- ifelse(RF$RF_P >0.5,1,0)
pred_test = prediction(RF$RF_out,train$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))
mean(auc_rf$V2)
table(RF$RF_out)
#---------------------------
revised_RF$count <- 0
revised_RF$sum_0 <- 0
revised_RF$sum_1 <- 0
for(i in 2:31){
  revised_RF$is = ifelse((revised_RF[,i] >=0.5) == T,1,0)
  revised_RF$count =  revised_RF$is +revised_RF$count
  revised_RF$sum_0[revised_RF$is==0]  = revised_RF[revised_RF$is==0,i] + revised_RF$sum_0[revised_RF$is==0] 
  revised_RF$sum_1[revised_RF$is==1]  = revised_RF[revised_RF$is==1,i] + revised_RF$sum_1[revised_RF$is==1]
}
revised_RF$RF_P <- revised_RF$sum_1/revised_RF$count
revised_RF$RF_P[revised_RF$count <= 27] <- revised_RF$sum_0[revised_RF$count <= 27]/
  (30-revised_RF$count[revised_RF$count <= 27])
revised_RF$RF_out <-  ifelse(revised_RF$RF_P >0.5,1,0)
table(revised_RF$RF_out)
rm(RF_model)
write.csv(revised_RF,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_RF_1.csv')
write.csv(RF,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\RF_1.csv')  

tijiao <- cbind(revised[73:76],revised_RF[,c(1,36)]) 
tijiao <- tijiao[-4]
tijiao <- tijiao[order(tijiao$`revised$revised_order`),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
tijiao <- sqldf("select User_id,Coupon_id,ymd,RF_P from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')  
########################################################################################
auc_Xgb <- data.frame()
Xgb <- as.data.frame( train$stack_order) 
revised_Xgb <- as.data.frame( revised$revised_order) 
n <- 1
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    traindata1 <- data.matrix(trainning[,c(1:72)])
    traindata2 <- Matrix(traindata1,sparse=T)
    traindata3 <- as.numeric(as.character( trainning[,73]) )
    traindata4 <- list(data=traindata2,label=traindata3) 
    xg_train <- xgb.DMatrix(data = traindata4$data, label = traindata4$label)
    size <- data.frame()  
    m <- 1
    train_midd <- as.data.frame( train$stack_order) 
    revised_midd <- as.data.frame( revised$revised_order) 
    for (k in seq(from=10,to=12,by=1)){
      for (q in seq(6,7,1)){
        testing = rbind(y_1[i,],y_0[-j,])
        testingdata1 <- data.matrix(testing[,c(1:72)]) 
        testingdata2 <- list(data = Matrix(testingdata1,sparse=T))
        xg_testing <- xgb.DMatrix(data = testingdata2$data)
        param <- list(booster = 'gbtree',max_depth = k, eta= 0.025,min_child_weight = q,
                      objective='binary:logistic',subsample = 0.7,colsample_bytree = 0.8)
        xg_model <- xgboost(params = param, data = xg_train,nrounds = 20000,
                            gamma = 0.2,seed = 2018,verbose = 0,reg_alpha = 100,
                            reg_lambda = 100,early_stopping_rounds = 100,eval_metric = 'auc')
        testing$pred = predict(xg_model,xg_testing)
        testing$out = 1
        testing$out[testing$pred < 0.5] = 0
        pred_test = prediction(testing$out,testing$y)
        perf_test = performance(pred_test,"tpr","fpr")
        test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
        print(c(k,q,test_auc))
        size[m,1] = k
        size[m,2] = q
        size[m,3] = test_auc
        m = m + 1
        
        testing = train
        testingdata1 <- data.matrix(testing[,c(1:72)]) 
        testingdata2 <- list(data = Matrix(testingdata1,sparse=T))
        xg_testing <- xgb.DMatrix(data = testingdata2$data)
        testing$pred = predict(xg_model,xg_testing)
        testing = subset(testing,select = c('stack_order','pred'))
        train_midd = merge(train_midd,testing,by.x = "train$stack_order",by.y = 'stack_order',all.x = T)
        reviseddata1 = data.matrix(revised[,c(1:72)]) 
        reviseddata2 = list(data = Matrix(reviseddata1,sparse=T))
        xg_revised = xgb.DMatrix(data = reviseddata2$data)
        revised_pred =  predict(xg_model,xg_revised)
        revised_pred = as.data.frame(revised_pred)
        revised_midd = cbind(revised_midd,revised_pred)
      }
    }
    names(size) <- c('max_depth','min_child_weight','test_auc')
    which.max(size$test_auc)
    
    print(c('test_AUC:',test_auc,'第',n,'个'))
    auc_Xgb[n,1] = n
    auc_Xgb[n,2] = test_auc
    n = n + 1
    
    testing = train_midd[,c(1, (which.max(size$test_auc)+1))]
    Xgb= merge(Xgb,testing,by = "train$stack_order",all.x = T)
    
    revised_pred = revised_midd[,(which.max(size$test_auc)+1)]
    revised_pred = as.data.frame(revised_pred)
    revised_Xgb = cbind(revised_Xgb,revised_pred)
  }
}

Xgb$count <- 0
Xgb$sum_0 <- 0
Xgb$sum_1 <- 0
for(i in 2:31){
  Xgb$is = ifelse( (Xgb[,i] >=0.5 & is.na(Xgb[,i]) != T) == T ,1,0)
  Xgb$is[is.na(Xgb[,i])] = 2
  Xgb$count =  Xgb$is +Xgb$count
  Xgb$sum_0[Xgb$is==0]  = Xgb[Xgb$is==0,i] + Xgb$sum_0[Xgb$is==0] 
  Xgb$sum_1[Xgb$is==1]  = Xgb[Xgb$is==1,i] + Xgb$sum_1[Xgb$is==1]
}
Xgb$Xgb_P <- Xgb$sum_1/Xgb$count
Xgb$Xgb_P[Xgb$count <= 24] <- Xgb$sum_0[Xgb$count <= 24]/
  (30 - Xgb$count[Xgb$count <= 24])
Xgb$Xgb_out <- ifelse(Xgb$Xgb_P >0.5,1,0)
pred_test = prediction(Xgb$Xgb_out,train$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))
mean(auc_Xgb$V2)
table(Xgb$Xgb_out)
#---------------------------
revised_Xgb$count <- 0
revised_Xgb$sum_0 <- 0
revised_Xgb$sum_1 <- 0
for(i in 2:31){
  revised_Xgb$is = ifelse((revised_Xgb[,i] >=0.5) == T,1,0)
  revised_Xgb$count =  revised_Xgb$is +revised_Xgb$count
  revised_Xgb$sum_0[revised_Xgb$is==0]  = revised_Xgb[revised_Xgb$is==0,i] + revised_Xgb$sum_0[revised_Xgb$is==0] 
  revised_Xgb$sum_1[revised_Xgb$is==1]  = revised_Xgb[revised_Xgb$is==1,i] + revised_Xgb$sum_1[revised_Xgb$is==1]
}
revised_Xgb$Xgb_P <- revised_Xgb$sum_1/revised_Xgb$count
revised_Xgb$Xgb_P[revised_Xgb$count <= 24] <- revised_Xgb$sum_0[revised_Xgb$count <= 24]/
  (30-revised_Xgb$count[revised_Xgb$count <= 24])
revised_Xgb$Xgb_out <-  ifelse(revised_Xgb$Xgb_P >0.5,1,0)
table(revised_Xgb$Xgb_out)
write.csv(revised_Xgb,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_Xgb_1.csv')
write.csv(Xgb,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\Xgb_1.csv')  
rm(rf_model,size,train_midd,traindata1,traindata2)
rm(traindata3,traindata4,xg_model,xg_revised,xg_train)
rm(xg_testing,i,j,k,m,n,q,test_auc,testingdata1,testingdata2)
rm(y_0,y_1,trainning,testing,reviseddata1,reviseddata2,mtry,folds_0,folds_1,param)
rm(revised_midd,revised_pred)

tijiao <- cbind(revised[73:76],revised_Xgb[,c(1,36)]) 
tijiao <- tijiao[-4]
tijiao <- tijiao[order(tijiao$`revised$revised_order`),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
tijiao <- sqldf("select User_id,Coupon_id,ymd,Xgb_P from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')  
##############################################################################

train_stack <- data.frame(LR[,c(1,36)],NNET=NNET$NNET_P,GBDT=GBDT$GBDT_P,
                          RF=RF$RF_P,XG=Xgb$Xgb_P)
train_stack <- cbind(train_stack,train)
train_stack <- train_stack[-1]
revised_stack <- data.frame(revised_LR[,c(1,36)],NNET=revised_NNET$NNET_P,
                            GBDT=revised_GBDT$GBDT_P,RF=revised_RF$RF_P,XG=revised_Xgb$Xgb_P)
revised_stack <- cbind(revised_stack,revised)
revised_stack <- revised_stack[-1]
train_stack$y <- as.factor(train_stack$y)
y_1 <- train_stack[train_stack$y==1,]
y_0 <- train_stack[train_stack$y==0,]
set.seed(2018)
folds_0 <- createFolds(y_0$y,k = 10)
str(folds_0)
folds_1 <- createFolds(y_1$y,k = 3)
str(folds_1)

AUC <- data.frame() 
LR_1 <- as.data.frame( train_stack$stack_order)  #  20次交叉验证后产生的结果，以后做训练集
revised_LR_1 <- as.data.frame( revised_stack$revised_order) 
n <- 1
for (i in folds_1){
  for (j in folds_0){
    trainning = rbind(y_1[-i,],y_0[j,])
    x = as.matrix(trainning[,c(1:5)])
    y = as.matrix(trainning[,78])
    lasso.cv = cv.glmnet(x = x,y = y,family = "binomial",alpha = 1,nfold = 5)
    lasso.min = glmnet(x = x,y = y,family = "binomial",alpha = 1,lambda = lasso.cv$lambda.min)
    
    testing = rbind(y_1[i,],y_0[-j,])
    test_x = as.matrix(testing[,c(1:5)]) 
    testing$LR_pred = predict(lasso.min,newx = test_x)
    testing$LR_p = 1/(1 + exp(-testing$LR_pred))
    testing$out = 1
    testing$out[testing$LR_p < 0.5] = 0
    pred_test = prediction(testing$out,testing$y)
    perf_test = performance(pred_test,"tpr","fpr")
    test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
    print(c('test_AUC:',test_auc))
    AUC[n,1] = n
    AUC[n,2] = test_auc
    n = n + 1
    testing = train_stack
    test_x = as.matrix(testing[,c(1:5)]) 
    testing$LR_pred = predict(lasso.min,newx = test_x)
    testing$LR_p = 1/(1 + exp(-testing$LR_pred))
    testing = subset(testing,select = c('stack_order','LR_p'))
    LR_1 = merge(LR_1,testing,by.x = "train_stack$stack_order",by.y = 'stack_order',all.x = T)
    revised_x = as.matrix(revised_stack[,c(1:5)])
    revised_pred = 1 / (1 + exp(-predict(lasso.min,newx = revised_x)))
    revised_pred = as.data.frame(revised_pred)
    revised_LR_1 = cbind(revised_LR_1,revised_pred)
  }
}
LR_1$count <- 0
LR_1$sum_0 <- 0
LR_1$sum_1 <- 0
for(i in 2:31){
  LR_1$is = ifelse( (LR_1[,i] >=0.5 & is.na(LR_1[,i]) != T) == T ,1,0)
  LR_1$is[is.na(LR_1[,i])] = 2
  LR_1$count =  LR_1$is +LR_1$count
  LR_1$sum_0[LR_1$is==0]  = LR_1[LR_1$is==0,i] + LR_1$sum_0[LR_1$is==0] 
  LR_1$sum_1[LR_1$is==1]  = LR_1[LR_1$is==1,i] + LR_1$sum_1[LR_1$is==1]
}
LR_1$LR_P <- LR_1$sum_1/LR_1$count
LR_1$LR_P[LR_1$count != 30] <- LR_1$sum_0[LR_1$count != 30]/(30-LR_1$count[LR_1$count != 30])
LR_1$LR_out <- ifelse(LR_1$LR_P >0.5,1,0)
pred_test = prediction(LR_1$LR_out,train_stack$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
table(LR_1$LR_out)
print(c('test_AUC:',test_auc))
mean(AUC$V2)
#---------------------------
revised_LR_1$count <- 0
revised_LR_1$sum_0 <- 0
revised_LR_1$sum_1 <- 0
for(i in 2:31){
  revised_LR_1$is = ifelse((revised_LR_1[,i] >=0.5) == T,1,0)
  revised_LR_1$count =  revised_LR_1$is +revised_LR_1$count
  revised_LR_1$sum_0[revised_LR_1$is==0]  = revised_LR_1[revised_LR_1$is==0,i] + revised_LR_1$sum_0[revised_LR_1$is==0] 
  revised_LR_1$sum_1[revised_LR_1$is==1]  = revised_LR_1[revised_LR_1$is==1,i] + revised_LR_1$sum_1[revised_LR_1$is==1]
}
revised_LR_1$LR_P <- revised_LR_1$sum_1/revised_LR_1$count
revised_LR_1$LR_P[revised_LR_1$count != 30] <- revised_LR_1$sum_0[revised_LR_1$count != 30]/
  (30-revised_LR_1$count[revised_LR_1$count != 30])
revised_LR_1$out <-  ifelse(revised_LR_1$LR_P >0.5,1,0)
table(revised_LR_1$out)
write.csv(revised_LR_1,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\revised_LR_2.csv')
write.csv(LR_1,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.27\\LR_2.csv')
rm(lasso.min,lasso.cv,revised_pred,test_x,revised_x,x,i,j,y)

tijiao <- cbind(revised_stack[78:81],revised_LR_1[,c(1,36)]) 
tijiao <- tijiao[-4]
tijiao <- tijiao[order(tijiao$`revised_stack$revised_order`),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
tijiao <- sqldf("select User_id,Coupon_id,ymd,LR_P from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')  
###################################################################################
train_stack$p <- ( train_stack$GBDT*5 + train_stack$XG *3  + train_stack$RF *2) / 10 
train_stack$out <- ifelse(train_stack$p > 0.5,1,0)
pred_test = prediction(train_stack$out,train_stack$y)
perf_test = performance(pred_test,"tpr","fpr")
test_auc = round(as.numeric(performance(pred_test,"auc")@y.values),5)
print(c('test_AUC:',test_auc))

revised_stack$p <- ( revised_stack$GBDT*5 + revised_stack$XG *3 + revised_stack$RF *2 ) / 10 
revised_stack$out <- ifelse(revised_stack$p > 0.5,1,0)
table(revised_stack$out)

tijiao <- revised_stack[,c(78:82)]
tijiao <- tijiao[order(tijiao$revised_order),]
tijiao$Date_received <- as.character(tijiao$Date_received )
tijiao$y <- substring(tijiao$Date_received,1,4)
tijiao$m <- substring(tijiao$Date_received,6,7)
tijiao$d <- substring(tijiao$Date_received,9,10)
tijiao$ymd <- paste(tijiao$y,tijiao$m,tijiao$d,sep = '')
tijiao <- sqldf("select User_id,Coupon_id,ymd,p from tijiao")
write.csv(tijiao,file = 'C:/Users/Administrator/Desktop/ensamble.csv')  
