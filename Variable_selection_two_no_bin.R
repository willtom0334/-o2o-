rm(list = ls())
require(discretization)
require(dplyr)
require(snowfall)
require(caret)
require(mice)
require(ClustOfVar)
require(reshape)
require(sqldf)
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.7.0_79\\jre')
memory.limit(102400)
sfInit( parallel=TRUE, cpus=2 )
offline <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/ccf_offline_stage1_train.csv")
first <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_1.csv')
first <- first[-1]
revised <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\revised_two_1.csv')
revised <- revised[-1]
# #------------加变量----------------------
######## 数据格式转化 ###########
offline$User_id <- as.character(offline$User_id)
offline$Merchant_id <- as.character(offline$Merchant_id)
offline$Coupon_id <- as.character((offline$Coupon_id))
offline$Discount_rate <- as.character(offline$Discount_rate)
offline$Distance_copy <- offline$Distance
offline$Date_received <- as.character(offline$Date_received)
offline$Date_received <- as.Date(offline$Date_received,'%Y%m%d')
offline$Date <- as.character(offline$Date)
offline$Date <- as.Date(offline$Date,'%Y%m%d')
offline$weekday_r <- weekdays(offline$Date_received)
######## 打标识 ############
offline$Discount_fac <- NA
offline$Discount_fac[grepl("^[z0-9]{1,4}\\:[z0-9]{1,4}$",offline$Discount_rate) == T] <- 1
offline$Discount_fac[grepl("^[z0]{1}\\.[z0-9]{1,2}$",offline$Discount_rate) == T] <- 0
offline$Discount_fac[offline$Discount_rate == "null"] <- 2
#0:折扣，1：满减，2：什么也不用的普通消费
offline$buy_fac <- NA
offline$buy_fac[(offline$Date - offline$Date_received) <= 15] <- 1
offline$buy_fac[(offline$Date - offline$Date_received) > 15] <- 0
offline$buy_fac[is.na(offline$Date) == T & is.na(offline$Date_received) == F] <- 2 
offline$buy_fac[is.na(offline$Date) == F & is.na(offline$Date_received) == T ] <- 3
# 0:领卷已消费超15天，1:领卷已消费15天内，2:领卷未消费，3:什么也用的普通正常消费
######## 分集 #########
offline <- tbl_df(offline)
train_offline_1 <- filter(offline,Date>='2016-01-01' & Date<='2016-04-30' & 
                            Date_received>='2016-01-01' & Date_received<='2016-04-30')
train_offline_2 <- filter(offline,Date_received>='2016-01-01' & Date_received<='2016-04-30',is.na(Date))
train_offline_3 <- filter(offline,Date>='2016-01-01' & Date<='2016-04-30',is.na(Date_received))
train_offline <- rbind(train_offline_1,train_offline_2,train_offline_3)



revised_offline_1 <- filter(offline,Date>='2016-02-16' & Date<='2016-06-15' & 
                              Date_received>='2016-02-16' & Date_received<='2016-06-15')
revised_offline_2 <- filter(offline,Date_received>='2016-02-16' & Date_received<='2016-06-15',is.na(Date))
revised_offline_3 <- filter(offline,Date>='2016-02-16' & Date<='2016-06-15',is.na(Date_received))
revised_offline <- rbind(revised_offline_1,revised_offline_2,revised_offline_3)
rm(revised_offline_1,revised_offline_2,revised_offline_3)
rm(train_offline_1,train_offline_2,train_offline_3)
gc()
#---------------is_cou_allbuy此卷是否全买，is_cou_allnobuy此卷是否全不买 --------
#---------------is_cou_less_than_3此卷是不是领卷小于10张的卷--------------
midd <- train_offline%>%select(Coupon_id,Date_received,Date)%>%filter(Coupon_id != 'null')
midd_1 <- midd%>%select(Coupon_id,Date_received)%>%group_by(Coupon_id)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Coupon_id,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Coupon_id)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Coupon_id',all.x = T)
cou_allbuy <- midd[which(midd$getnumber == midd$buynumber),]
cou_allbuy <- as.character(cou_allbuy$Coupon_id)
cou_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
cou_allnobuy <- as.character(cou_allnobuy$Coupon_id)
cou_less_than_3 <- midd[which(midd$getnumber <10 ),]
cou_less_than_3 <- as.character(cou_less_than_3$Coupon_id)
first$is_cou_allbuy <- ifelse(first$Coupon_id %in% cou_allbuy,1,0)
first$is_cou_allnobuy <- ifelse(first$Coupon_id %in% cou_allnobuy,1,0)
first$cou_less_than_3 <- ifelse(first$Coupon_id %in% cou_less_than_3,1,0 )

midd <- revised_offline%>%select(Coupon_id,Date_received,Date)%>%filter(Coupon_id != 'null')
midd_1 <- midd%>%select(Coupon_id,Date_received)%>%group_by(Coupon_id)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Coupon_id,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Coupon_id)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Coupon_id',all.x = T)
cou_allbuy <- midd[which(midd$getnumber == midd$buynumber),]
cou_allbuy <- as.character(cou_allbuy$Coupon_id)
cou_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
cou_allnobuy <- as.character(cou_allnobuy$Coupon_id)
cou_less_than_3 <- midd[which(midd$getnumber <10 ),]
cou_less_than_3 <- as.character(cou_less_than_3$Coupon_id)
revised$is_cou_allbuy <- ifelse(revised$Coupon_id %in% cou_allbuy,1,0)
revised$is_cou_allnobuy <- ifelse(revised$Coupon_id %in% cou_allnobuy,1,0)
revised$cou_less_than_3 <- ifelse(revised$Coupon_id %in% cou_less_than_3,1,0 )
#---------------is_Mer_allbuy此供应商是否全买，is_Mer_allnobuy此供应商是否全不买 --------
#---------------is_Mer_less_than_3此供应商是不是领卷小于10张的卷--------------
midd <- train_offline%>%select(Merchant_id,Date_received,Date)%>%filter(is.na(Date_received)!=T)
midd_1 <- midd%>%select(Merchant_id,Date_received)%>%group_by(Merchant_id)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Merchant_id,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Merchant_id)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Merchant_id',all.x = T)
Mer_allbuy <- midd[which(midd$getnumber == midd$buynumber),]
Mer_allbuy <- as.character(Mer_allbuy$Merchant_id)
Mer_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
Mer_allnobuy <- as.character(Mer_allnobuy$Merchant_id)
Mer_less_than_3 <- midd[which(midd$getnumber <10 ),]
Mer_less_than_3 <- as.character(Mer_less_than_3$Merchant_id)
first$is_Mer_allbuy <- ifelse(first$Merchant_id %in% Mer_allbuy,1,0)
first$is_Mer_allnobuy <- ifelse(first$Merchant_id %in% Mer_allnobuy,1,0)
first$Mer_less_than_3 <- ifelse(first$Merchant_id %in% Mer_less_than_3,1,0 )

midd <- revised_offline%>%select(Merchant_id,Date_received,Date)%>%filter(is.na(Date_received)!=T)
midd_1 <- midd%>%select(Merchant_id,Date_received)%>%group_by(Merchant_id)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Merchant_id,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Merchant_id)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Merchant_id',all.x = T)
Mer_allbuy <- midd[which(midd$getnumber == midd$buynumber),]
Mer_allbuy <- as.character(Mer_allbuy$Merchant_id)
Mer_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
Mer_allnobuy <- as.character(Mer_allnobuy$Merchant_id)
Mer_less_than_3 <- midd[which(midd$getnumber <10 ),]
Mer_less_than_3 <- as.character(Mer_less_than_3$Merchant_id)
revised$is_Mer_allbuy <- ifelse(revised$Merchant_id %in% Mer_allbuy,1,0)
revised$is_Mer_allnobuy <- ifelse(revised$Merchant_id %in% Mer_allnobuy,1,0)
revised$Mer_less_than_3 <- ifelse(revised$Merchant_id %in% Mer_less_than_3,1,0 )
#---------------is_dis_allbuy此供应商是否全买，is_dis_allnobuy此供应商是否全不买 --------
#---------------is_dis_less_than_3此供应商是不是领卷小于10张的卷--------------
midd <- train_offline%>%select(Discount_rate,Date_received,Date)%>%filter(is.na(Date_received)!=T)
midd_1 <- midd%>%select(Discount_rate,Date_received)%>%group_by(Discount_rate)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Discount_rate,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Discount_rate)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Discount_rate',all.x = T)
dis_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
dis_allnobuy <- as.character(dis_allnobuy$Discount_rate)
dis_less_than_3 <- midd[which(midd$getnumber <50 ),]
dis_less_than_3 <- as.character(dis_less_than_3$Discount_rate)
first$is_dis_allnobuy <- ifelse(first$Discount_rate %in% dis_allnobuy,1,0)
first$dis_less_than_3 <- ifelse(first$Discount_rate %in% dis_less_than_3,1,0 )

midd <- revised_offline%>%select(Discount_rate,Date_received,Date)%>%filter(is.na(Date_received)!=T)
midd_1 <- midd%>%select(Discount_rate,Date_received)%>%group_by(Discount_rate)%>%summarise(getnumber = n())
midd_2 <- midd%>%select(Discount_rate,Date)%>%filter(is.na(Date)!=T)%>%
  group_by(Discount_rate)%>%summarise( buynumber= n())
midd <- merge(midd_1,midd_2,by = 'Discount_rate',all.x = T)
dis_allnobuy <- midd[which(is.na(midd$buynumber) == T),]
dis_allnobuy <- as.character(dis_allnobuy$Discount_rate)
dis_less_than_3 <- midd[which(midd$getnumber <50 ),]
dis_less_than_3 <- as.character(dis_less_than_3$Discount_rate)
revised$is_dis_allnobuy <- ifelse(revised$Discount_rate %in% dis_allnobuy,1,0)
revised$dis_less_than_3 <- ifelse(revised$Discount_rate %in% dis_less_than_3,1,0 )

# #-------------------预测集中客户所领的特殊号码，占所有号码的比例-----------------------------------------
midd <- first%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( C_count = n())
midd_1 <- first%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id)%>%
  summarise( total_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$U_C_rate <- midd$C_count / midd$total_count
first <- merge(first,midd,by = c('User_id','Coupon_id'),all.x = T) 
# #-------------------预测集中客户所领的供应商，占所有供应商的比例-----------------------------------------
midd <- first%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( M_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$M_C_rate <- midd$M_count / midd$total_count
midd <- midd[-4]
first <- merge(first,midd,by = c('User_id','Merchant_id'),all.x = T) 
# #-------------------预测集中客户所领的折扣，占所有折扣的比例-----------------------------------------
midd <- first%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( D_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$D_C_rate <- midd$D_count / midd$total_count
midd <- midd[-4]
first <- merge(first,midd,by = c('User_id','Discount_rate'),all.x = T) 
#---------------------预测集中客户星期几领的卷，占所有的比例-----------------------------------
midd <- first%>%select(User_id,is_weekday,Date_received)%>%group_by(User_id,is_weekday)%>%
  summarise( W_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$W_C_rate <- midd$W_count / midd$total_count
midd <- midd[-4]
first <- merge(first,midd,by = c('User_id','is_weekday'),all.x = T) 
#---------------------预测集中客户按距离远近占所有的比例-----------------------------------
midd <- first%>%select(User_id,Distance,Date_received)%>%group_by(User_id,Distance)%>%
  summarise( Distance_count = n())

midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$Distance_C_rate <- midd$Distance_count / midd$total_count
midd <- midd[-4]
first <- merge(first,midd,by = c('User_id','Distance'),all.x = T)
# #------------------提交集处理-----------------------------------------
midd <- revised%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( C_count = n())
midd_1 <- revised%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id)%>%
  summarise( total_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$U_C_rate <- midd$C_count / midd$total_count
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T) 
midd <- revised%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( M_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$M_C_rate <- midd$M_count / midd$total_count
midd <- midd[-4]
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T) 
midd <- revised%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( D_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$D_C_rate <- midd$D_count / midd$total_count
midd <- midd[-4]
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T) 
midd <- revised%>%select(User_id,is_weekday,Date_received)%>%group_by(User_id,is_weekday)%>%
  summarise( W_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$W_C_rate <- midd$W_count / midd$total_count
midd <- midd[-4]
revised <- merge(revised,midd,by = c('User_id','is_weekday'),all.x = T) 
midd <- revised%>%select(User_id,Distance,Date_received)%>%group_by(User_id,Distance)%>%
  summarise( Distance_count = n())
midd <- merge(midd,midd_1,by='User_id',all.x = T)
midd$Distance_C_rate <- midd$Distance_count / midd$total_count
midd <- midd[-4]
revised <- merge(revised,midd,by = c('User_id','Distance'),all.x = T)
write.csv(revised,"E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/revised_two_2.csv")
write.csv(first,"E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/train_two_2.csv")
#######################################################################################################
rm(list = ls())
first <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_2.csv')
first <- first[-c(1:2,5:9)]
id <- duplicated(first)
table(id)
first <- first[!id,]
table(first$y)
rm(id)
md.pattern(first) 
fac <- first[c(3,4,33,83:90,143,145,147,149,151,153,166,168,170,175,177,179,191:210)]
num <- first[c(3,1:2,5:32,34:82,91:142,144,146,148,150,152,154:165,167,169,171:174,176,178,180:190,211:221)]
########################统计推断############################################
num$y <- as.factor(num$y)
#-------------------两样本T检验-----------------------
n <- ncol(num)   #样本量21万，P值意义不大，分层抽样4000个样本做统计推断
n1 <- data.frame(names(num)[2:n])
for (i in 2:6){
  id <- createDataPartition(y = num$y,p = 0.02,list = F)  #不给种子随机抽
  sam <- num[id,]
  for (j in 2:n) {
    t = t.test(sam[,j] ~ sam[,1],equal=T)                
    n1[j-1,i] = as.numeric(t[["p.value"]])
    j = j + 1
  }
}
n1[is.na(n1) == T] <- 1
for (i in 1:(n-1)){
  x = 0
  for (j in 2:6){
    if (n1[i,j] < 5e-02){
      x = x + 1
    }else
    {next}
  }
  if (x >= 3){
    n1[i,7] = '0'
  }else
  { n1[i,7] = '1'}   #1:无关 
  i = i + 1
}
names(n1) <- c("Var_name","t.p.value_1","t.p.value_2","t.p.value_3","t.p.value_4","t.p.value_5","Result")
n2 <- n1[n1$Result == 1,] 
#-------------------wilcox-----------------------
n3 <- data.frame(names(num)[2:n])
for (i in 2:6){
  id <- createDataPartition(y = num$y,p = 0.02,list = F)  #不给种子随机抽
  sam <- num[id,]
  for (j in 2:n) {
    wilcox = wilcox.test(sam[,j] ~ sam[,1],equal=T)                
    n3[j-1,i] = as.numeric(wilcox[["p.value"]])
    j = j + 1
  }
}
n3[is.na(n3) == T] <- 1
for (i in 1:(n-1)){
  x = 0
  for (j in 2:6){
    if (n3[i,j] < 5.0e-02){
      x = x + 1
    }else
    {next}
  }
  if (x >= 3){
    n3[i,7] = '0'
  }else
  { n3[i,7] = '1'}   #1:无关 
  i = i + 1
}
names(n3) <- c("Var_name","wilcox.p.value_1","wilcox.p.value_2","wilcox.p.value_3","wilcox.p.value_4","wilcox.p.value_5","Result")
n4 <- n3[n3$Result == 1,] 
#-------------------------------对比分析，两种检验方法的不同-----------------------------
diff <- data.frame(intersect(n2$Var_name,n4$Var_name))
print(setdiff(n2$Var_name,n4$Var_name))  #差集先留着
print(setdiff(n4$Var_name,n2$Var_name))

diff_1 <- as.character(diff$intersect.n2.Var_name..n4.Var_name.)
diff_2 <- setdiff(names(num),diff_1)
#----------------------------------------------------
num <- num[diff_2] #数值变是统计推断完成
rm(diff,n1,n2,n3,n4,t,wilcox,diff_1,diff_2,i,j,n,x)
#-------------------------------卡方检验--------------------------------------------------
#  这里is_new_discount_rate变量，因数量太少，有时抽样抽不到，卡方检就会报错
fac <- subset(fac,select = -c(is_new_discount_rate))
for (i in 1:ncol(fac)){fac[,i] <- as.factor(fac[,i])}
n <- ncol(fac)   
n5 <- data.frame(names(fac)[2:n])
for (i in 2:6){
  id <- createDataPartition(y = fac$y,p = 0.02,list = F)  
  sam <- fac[id,]
  for (j in 2:n) {
    chisq = chisq.test(x = sam[,j] , y = sam$y)                
    n5[j-1,i] = chisq[["p.value"]]
    j = j + 1
  }
}
for (i in 1:(n-1)){
  x = 0
  for (j in 2:6){
    if (n5[i,j] < 5.0e-02){
      x = x + 1
    }else
    {next}
  }
  if (x >= 3){
    n5[i,7] = '0'
  }else
  { n5[i,7] = '1'}    
  i = i + 1
}
names(n5) <- c("Var_name","chisq.p.value_1","chisq.p.value_2","chisq.p.value_3","chisq.p.value_4","chisq.value_5","Result")
n6 <- n5[n5$Result == 1,]
setdiff(names(fac),n6$Var_name)

fac <- fac[setdiff(names(fac),n6$Var_name)]   
rm(n5,n6,id,sam,chisq,i,j,n,x)
num <- num[-1]
second <- cbind(fac,num)  
rm(fac,num,first)
gc()
write.csv(second,"E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/train_two_second.csv")
##############################################################
second <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/train_two_second.csv")
second <- second[-1]
# 先筛一部分，否则21万条150个变量，聚10个小时聚不出来
for (i in 1:ncol(second)){second[,i] <- as.numeric(as.character(second[,i]))}
names(second)[1] <- 'sec_y'
second$y<- second$sec_y #，要求最后一列是分类属性
second <- second[-1]
second$y <- as.factor(second$y)   
chiq <- chiM(second,alpha=0.05)
chiq_data <- chiq[["Disc.data"]]
N_0 = table(chiq_data[,'y'])[1]
N_1 = table(chiq_data[,'y'])[2]
iv_c = NULL
var_c = NULL
for (col in colnames(chiq_data)){
  if ( col != 'y') {
    frq = as.data.frame(table(chiq_data[, col], chiq_data[, 'y']))
    len = length(unique(frq$Var1))
    iv = 0
    for (i in 1:len){
      N_i_0 = frq$Freq[frq$Var1==i & frq$Var2==0]
      N_i_1 = frq$Freq[frq$Var1==i & frq$Var2==1]
      iv = iv+(N_i_0/N_0- N_i_1/N_1)*log((N_i_0/N_0 + 1e-04)/(N_i_1/N_1 + 1e-04))
    }
    iv_c = c(iv_c, iv)
    var_c = c(var_c, col)
  }
}
iv_df <- data.frame(var=var_c, iv=iv_c, stringsAsFactors = FALSE)
iv_df <- iv_df[order(iv_df$iv,decreasing = T),]
iv_df$seq <- 1:nrow(iv_df)
iv_df <- iv_df[which(iv_df$seq <= 128),]
vari <- iv_df$var
vari <- as.character(vari)
vari[length(vari)+1] <- 'y'
three <- second[vari]
id <- duplicated(three)
table(id)
three <- three[!id,]
rm(col,i,iv,chiq,chiq_data,frq,iv_df,id,iv_c,len,N_0,N_1,N_i_0,N_i_1)
write.csv(three,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_three.csv')
#################################################################
three <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_three.csv')
three <- three[-1]
for (i in 1:ncol(three)){three[,i] <- as.numeric(as.character(three[,i]))}
#tree函数要求变量必须全是数值值
tree <- hclustvar(three)
st <- stability(tree,B=20)
st[["meanCR"]]
tree_number <- unlist(st[["meanCR"]])
tree_number <- sort(tree_number,decreasing = T)
print(tree_number)

part <- cutreevar(tree,73,matsim = T)  
print(part$sim) 
cluster <- part[["size"]]
cluster <- cluster[cluster != 1]
cluster <- names(cluster)
cluster <- substring(cluster,8,11)
clu_list <- as.numeric(cluster)

for (i in 1:ncol(three)){three[,i] <- as.numeric(as.character(three[,i]))}
three$y <- as.factor(three$y)
chiq <- chiM(three,alpha=0.05)
#参数1是数据框名，参数2是卡方P_Value 第一个参数data，是输入数据集，要求最后一列是分类属性。
chiq_data <- chiq[["Disc.data"]]
N_0 = table(chiq_data[,'y'])[1]
N_1 = table(chiq_data[,'y'])[2]
iv_c = NULL
var_c = NULL
for (col in colnames(chiq_data)){
  if ( col != 'y') {
    frq = as.data.frame(table(chiq_data[, col], chiq_data[, 'y']))
    len = length(unique(frq$Var1))
    iv = 0
    for (i in 1:len){
      N_i_0 = frq$Freq[frq$Var1==i & frq$Var2==0]
      N_i_1 = frq$Freq[frq$Var1==i & frq$Var2==1]
      iv = iv+(N_i_0/N_0- N_i_1/N_1)*log((N_i_0/N_0 + 1e-04)/(N_i_1/N_1 + 1e-04))
    }
    iv_c = c(iv_c, iv)
    var_c = c(var_c, col)
  }
}
iv_df <- data.frame(var=var_c, iv=iv_c, stringsAsFactors = FALSE)
iv_df <- iv_df[order(iv_df$iv,decreasing = T),]
print(iv_df)


Ratio <- c()
for (i in clu_list){
  x = paste('cluster',i,sep = '')       #求同第多少类名: cluster6
  names = rownames(as.data.frame(part$sim[x]))  #求第6类的所有变量名
  n = nrow(as.data.frame(part$sim[x]))  #求第 x类有几个变量
  c = c()
  for (j in 1:n){
    c[j] =  iv_df$iv[iv_df$var == names[j]] 
  }
  Ratio[i] =  names[which.max(c)]
  
  Ratio = as.vector(na.omit(Ratio))  #返回留下的变量
  next
}
print(Ratio)  #返回留下的变量
n <- as.numeric(as.character(part[["wss"]])) #注意要先返回字符串才是真实值，只是近似1，而不是直正的1
n <- which(round(n,digits = 5) == 1)
vari <- c()
for (i in n){
  a = rownames(as.data.frame(part$sim[i]))
  vari = c(vari,a)
} #把所有唯一变量名，取出来
vari <- c(Ratio,vari)   #这是最终留下的  经过聚类后的变量

fourth <- three[vari] 
id <- duplicated(fourth)
table(id)
fourth <- fourth[!id,]
write.csv(fourth,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_fourth.csv')
rm(col,i,iv,chiq,chiq_data,frq,iv_df,id,iv_c,len,N_0,N_1,N_i_0,N_i_1)
#################################################################
five <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.19\\train_two_fourth.csv')
five <- five[-1]
five$y <- as.factor(five$y)

chiq <- chiM(five,alpha=0.05)
#参数1是数据框名，参数2是卡方P_Value 第一个参数data，是输入数据集，要求最后一列是分类属性。
chiq_data <- chiq[["Disc.data"]]
N_0 = table(chiq_data[,'y'])[1]
N_1 = table(chiq_data[,'y'])[2]
iv_c = NULL
var_c = NULL
for (col in colnames(chiq_data)){
  if ( col != 'y') {
    frq = as.data.frame(table(chiq_data[, col], chiq_data[, 'y']))
    len = length(unique(frq$Var1))
    iv = 0
    for (i in 1:len){
      N_i_0 = frq$Freq[frq$Var1==i & frq$Var2==0]
      N_i_1 = frq$Freq[frq$Var1==i & frq$Var2==1]
      iv = iv+(N_i_0/N_0- N_i_1/N_1)*log((N_i_0/N_0 + 1e-04)/(N_i_1/N_1 + 1e-04))
    }
    iv_c = c(iv_c, iv)
    var_c = c(var_c, col)
  }
}
iv_df <- data.frame(var=var_c, iv=iv_c, stringsAsFactors = FALSE)
iv_df <- iv_df[order(iv_df$iv,decreasing = T),]
print(iv_df)


#-----------------------------------------
revised <- read.csv('E:\\R 3.4.2 for Windows\\O2O_tc\\revised_two.csv')
revised_vari <- names(five)
revised_vari <- revised_vari[-length(revised_vari)]
revised_vari <- c(revised_vari,'User_id','Coupon_id','Date_received','revised_order')
revised <- revised[revised_vari]


write.csv(revised,file = 'E:\\R 3.4.2 for Windows\\O2O_tc\\2019.1.14\\revised_two_fourth.csv')       
