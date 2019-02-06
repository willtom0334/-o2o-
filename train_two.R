################################### Train & Revised ####################################
rm(list = ls())
require(dplyr)
require(reshape)
require(tidyr)
require(sqldf)
require(snowfall)
require(smbinning)
setwd('C:/Users/Administrator/Desktop/tc')
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.7.0_79\\jre')
memory.limit(102400)
sfInit( parallel=TRUE, cpus=2 )
offline <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/ccf_offline_stage1_train.csv")#175万，7个变量
revised <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/ccf_offline_stage1_test_revised.csv")
revised$revised_order <- seq(1,113640,1)  #加一列，提交时与原顺序保持一致
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
print(c(min(train_offline$Date_received,na.rm = T), max(train_offline$Date_received,na.rm = T)))
print(c(min(train_offline$Date,na.rm = T), max(train_offline$Date,na.rm = T))) 
train <- filter(offline,Date_received>='2016-05-16' & Date_received<='2016-06-15')
print(c(min(train$Date_received,na.rm = T), max(train$Date_received,na.rm = T)))


revised_offline_1 <- filter(offline,Date>='2016-02-16' & Date<='2016-06-15' & 
                              Date_received>='2016-02-16' & Date_received<='2016-06-15')
revised_offline_2 <- filter(offline,Date_received>='2016-02-16' & Date_received<='2016-06-15',is.na(Date))
revised_offline_3 <- filter(offline,Date>='2016-02-16' & Date<='2016-06-15',is.na(Date_received))
revised_offline <- rbind(revised_offline_1,revised_offline_2,revised_offline_3)
print(c(min(revised_offline$Date_received,na.rm = T), max(revised_offline$Date_received,na.rm = T)))
print(c(min(revised_offline$Date,na.rm = T), max(revised_offline$Date,na.rm = T))) 
revised$User_id <- as.character(revised$User_id)
revised$Merchant_id <- as.character(revised$Merchant_id)
revised$Coupon_id <- as.character((revised$Coupon_id))
revised$Discount_rate <- as.character(revised$Discount_rate)
revised$Date_received <- as.character(revised$Date_received)
revised$Date_received <- as.Date(revised$Date_received,'%Y%m%d')
print(c(min(revised$Date_received,na.rm = T), max(revised$Date_received,na.rm = T)))
rm(revised_offline_1,revised_offline_2,revised_offline_3)
rm(train_offline_1,train_offline_2,train_offline_3)
gc()
########### 训练集及提交集 distance，y 及排序处理 ###############
train$Distance_copy <- train$Distance
train$Distance <- ordered(train$Distance,
                          levels = c('0','1','2','3','4','5','6','7','8','9','10','null'))
train$Distance <- as.character(train$Distance)
train$Distance[train$Distance == 'null'] <- NA
train$Distance <- as.numeric(train$Distance)   #直接转会因排序问题引起数据乱，故先排序再字符再数值
median(train$Distance,na.rm = T)  #1
train$Distance[is.na(train$Distance) == T] <- 0 #用中位数0补
train$y <- ifelse(train$buy_fac == 1,1,0)
train <- sqldf("select  User_id,Merchant_id,Coupon_id,Discount_rate,Date_received,weekday_r,
               y,Distance,Discount_fac,Distance_copy from train")
#------------------------------------
revised$Distance_copy <- revised$Distance
revised$Distance <- ordered(revised$Distance,
                            levels = c('0','1','2','3','4','5','6','7','8','9','10','null'))
revised$Distance <- as.character(revised$Distance)
revised$Distance[revised$Distance == 'null'] <- NA
revised$Distance <- as.numeric(revised$Distance)  
revised$Distance[is.na(revised$Distance) == T] <- 0  #用训练集中位数补
revised$weekday_r <- weekdays(revised$Date_received)
revised$Discount_fac[grepl("^[z0-9]{1,4}\\:[z0-9]{1,4}$",revised$Discount_rate) == T] <- 1
revised$Discount_fac[grepl("^[z0]{1}\\.[z0-9]{1,2}$",revised$Discount_rate) == T] <- 0
revised <- sqldf("select  User_id,Merchant_id,Coupon_id,Discount_rate,Date_received,weekday_r,
                 revised_order,Distance,Discount_fac,Distance_copy from revised")
#################################  变量提取  ############################################
########### 整个期间与Coupon_id相关###################
#---特殊号码的优惠卷被领取的总次数 Coupon_get_count---
midd <- train_offline%>%select(Coupon_id,Date_received)%>%filter(Coupon_id != 'null')%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_count = n())
train <- merge(train,midd,by = 'Coupon_id',all.x = T)
train$Coupon_get_count[is.na(train$Coupon_get_count) == T] <- 0
#----客户领取特殊号码的优惠卷未购买的次数 Coupon_get_no_buy_count----
midd <- train_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_no_buy_count = n())
train <- merge(train,midd,by = 'Coupon_id',all.x = T)
train$Coupon_get_no_buy_count[is.na(train$Coupon_get_no_buy_count) == T] <- 0
#----客户领取特殊号码的优惠卷并且已经购买的次数 Coupon_get_buy_count-----------------
midd <- train_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_buy_count = n())
train <- merge(train,midd,by = 'Coupon_id',all.x = T)
train$Coupon_get_buy_count[is.na(train$Coupon_get_buy_count) == T] <- 0
#---客户领取特殊号码优惠卷并且在15日内购买的次数 Coupon_get_buy_15----------
midd <- train_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_buy_15 = n())
train <- merge(train,midd,by = 'Coupon_id',all.x = T)
train$Coupon_get_buy_15[is.na(train$Coupon_get_buy_15) == T] <- 0
#---客户领取特殊号码的优惠卷并购买的次数  但不在15日内（线下）Coupon_get_buy_not15-----
train$Coupon_get_buy_not15 <- train$Coupon_get_buy_count - train$Coupon_get_buy_15
#----特殊号码优惠卷的负样本占比（领卷已购买但不在15天之内及领了就没买）Coupon_get_negative_rate --
train$Coupon_get_negative_rate <- round( (train$Coupon_get_buy_not15 + train$Coupon_get_no_buy_count) / 
                                           train$Coupon_get_count,digits = 3)
train$Coupon_get_negative_rate[is.na(train$Coupon_get_negative_rate) == T] <- 0.000
#-----特殊号码优惠卷 已领卷但在15日后购买 占比 Coupon_get_negative_15rate ------
train$Coupon_get_negative_15rate <- round( train$Coupon_get_buy_not15 / train$Coupon_get_count,digits = 3)
train$Coupon_get_negative_15rate[is.na(train$Coupon_get_negative_15rate) == T] <- 0.000
#-----特殊号码优惠卷 正样本 （领取并在15日内购买）占比 Coupon_positive_rate------------------
train$Coupon_positive_rate<- round(train$Coupon_get_buy_15 / train$Coupon_get_count,digits = 3)
train$Coupon_positive_rate[is.na(train$Coupon_positive_rate) == T] <- 0.000
########### 整个期间与领取购买相关历史记录 ################
#---客户领取优惠卷的总次数（线下）get_count_off----    
midd <- train_offline%>%select(User_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id)%>%summarise(get_count_off=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_count_off[is.na(train$get_count_off) == T] <- 0
#----客户领取优惠卷未购买的次数（线下）get_no_buy_off----
midd <- train_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id)%>%summarise(get_no_buy_off=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_no_buy_off[is.na(train$get_no_buy_off) == T] <- 0
#----客户领取优惠卷并且已经购买的次数（线下）get_buy_off-----------------
midd <- train_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id)%>%summarise(get_buy_off=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_buy_off[is.na(train$get_buy_off) == T] <- 0
#-------------客户什么卷也不用的普通购买次数（线下）nodiscount_buy_off--------
midd <- train_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id)%>%summarise(nodiscount_buy_off = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$nodiscount_buy_off[is.na(train$nodiscount_buy_off) == T] <- 0
#------客户领取优惠卷并且在15日内购买的次数（线下）get_buy_off15----------
midd <- train_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id)%>%summarise(get_buy_off15=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_buy_off15[is.na(train$get_buy_off15) == T] <- 0
#------客户领取优惠卷并购买的次数  但不在15日内（线下）get_buy_off_not15----------
train$get_buy_off_not15 <- train$get_buy_off - train$get_buy_off15
#------------客户领取满减卷的次数（线下）get_mj_off---------------
midd <- train_offline%>%select(User_id,Discount_fac)%>%filter(Discount_fac == 1)%>%
  group_by(User_id)%>%summarise(get_mj_off = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_mj_off[is.na(train$get_mj_off) == T] <- 0
#-----------客户领取折扣卷的次数（线下） get_zk_off---------------------
midd <- train_offline%>%select(User_id,Discount_fac)%>%filter(Discount_fac == 0)%>%
  group_by(User_id)%>%summarise(get_zk_off = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_zk_off[is.na(train$get_zk_off) == T] <- 0
#----------------客户使用满减卷购买的总次数（线下） mj_buy_off-------------------------
midd <- train_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 1 )%>%group_by(User_id)%>%summarise( mj_buy_off = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$mj_buy_off[is.na(train$mj_buy_off) == T] <- 0
#----------------客户使用满减卷并在15日内购买的次数（线下） mj_buy_off15-------------------------
midd <- train_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 1 )%>%group_by(User_id)%>%summarise( mj_buy_off15 = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$mj_buy_off15[is.na(train$mj_buy_off15) == T] <- 0
#----------------客户使用折扣卷购买的总次数（线下） zk_buy_off---------------
midd <- train_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 0 )%>%group_by(User_id)%>%summarise( zk_buy_off = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$zk_buy_off[is.na(train$zk_buy_off) == T] <- 0
#----------------客户使用折扣卷并在15日内购买的总次数（线下） zk_buy_off15---------------
midd <- train_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 0 )%>%group_by(User_id)%>%summarise( zk_buy_off15 = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$zk_buy_off15[is.na(train$zk_buy_off15) == T] <- 0
#-----------客户 满减卷正样本 占比 mj_positive_rate------------------
train$mj_positive_rate <- round(train$mj_buy_off15 / train$get_mj_off,digits = 3)
train$mj_positive_rate[is.na(train$mj_positive_rate) == T] <- 0.000
#-----------客户 折扣卷正样本 占比 zk_positive_rate------------------
train$zk_positive_rate <- round(train$zk_buy_off15 / train$get_zk_off,digits = 3)
train$zk_positive_rate[is.na(train$zk_positive_rate) == T] <- 0.000
#-----------客户 无卷购买占比 nodiscount_rate-------------------
train$nodiscount_rate <- round(train$nodiscount_buy_off / 
                                 (train$get_buy_off + train$nodiscount_buy_off),digits = 3)
train$nodiscount_rate[is.na(train$nodiscount_rate) == T] <- 0.000
#-----------客户 用卷正样本 占比 get_positive_rate-------------------
train$get_positive_rate <- round(train$get_buy_off15 / train$get_count_off,digits = 3)
train$get_positive_rate[is.na(train$get_positive_rate) == T] <- 0.000
#-----------客户 用卷负样本样本（领卷已购买但不在15天之内及领了就没买） 占比 get_negative_rate ------
train$get_negative_rate <- round( (train$get_buy_off_not15 + train$get_no_buy_off) / 
                                    train$get_count_off,digits = 3)
train$get_negative_rate[is.na(train$get_negative_rate) == T] <- 0.000
#-----------客户 用卷15日后购买 占比 get_negative_15rate ------
train$get_negative_15rate <- round( train$get_buy_off_not15 / train$get_count_off,digits = 3)

train$get_negative_15rate[is.na(train$get_negative_15rate) == T] <- 0.000
#----------客户 正样本 与 总购买次数的占比 buy_positive_rate------------------
train$buy_positive_rate <- round(train$get_buy_off15 /
                                   (train$nodiscount_buy_off + train$get_buy_off),digits = 3)
train$buy_positive_rate[is.na(train$buy_positive_rate) == T] <- 0.000
gc()
################### 与日期相关的购买 ############################
#-----------------------客户平均购买时间间隔（所有购买）---------------------
midd <- train_offline%>%select(User_id,Date)%>%group_by(User_id)%>%summarise( min_Date = min(Date,na.rm = T),
                                                                              max_Date = max(Date,na.rm = T))
midd$avg_datediff_off <- midd$max_Date-midd$min_Date
midd <- midd[-c(2,3)]
train <- merge(train,midd,by = 'User_id',all.x = T)
midd <- train_offline%>%select(User_id,buy_fac)%>%filter(buy_fac != 2)%>%
  group_by(User_id)%>%summarise(cs = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$avg_datediff_off <- round(train$avg_datediff_off/train$cs,digits = 2)
train <- subset(train, select = -c(cs) ) #用变量名取子集
train$avg_datediff_off[is.na(train$avg_datediff_off) == T] <- 0
#------------------客户领取优惠卷的当日是本周的第几天 isweekday----------
train$weekday_r <- as.factor(train$weekday_r)
train$weekday_r <- ordered(train$weekday_r,levels = c('星期一','星期二','星期三','星期四',
                                                      '星期五','星期六','星期日'))
train$is_weekday <- unclass(train$weekday_r)
#------------------客户领取优惠卷的当日是否周末 is_weekend------------
train$is_weekend <- 0
train$is_weekend <- ifelse(train$is_weekday == 6 | train$is_weekday == 7,1,0)
train$is_weekend <- as.numeric(train$is_weekend)
#-------------客户周一领卷并购买的次数（线下） received_mon 及 mon_rate率------------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期一' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_mon = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_mon[is.na(train$received_mon) == T] <- 0
train$mon_rate <- round(train$received_mon / train$get_buy_off,digits = 3)
train$mon_rate[is.na(train$mon_rate) == T] <- 0.000
#---------------0客户周二领并购买卷的次数（线下）  received_tur 及 tur_rate率-----------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期二' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_tur = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_tur[is.na(train$received_tur) == T] <- 0
train$tur_rate <- round(train$received_tur / train$get_buy_off,digits = 3)
train$tur_rate[is.na(train$tur_rate) == T] <- 0.000
#-----------客户周三领卷并购买的次数（线下）  received_wen 及 wen_rate率-------------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期三' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_wen = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_wen[is.na(train$received_wen) == T] <- 0
train$wen_rate <- round(train$received_wen / train$get_buy_off,digits = 3)
train$wen_rate[is.na(train$wen_rate) == T] <- 0.000
#---------------客户周四领卷并购买的次数（线下）   received_thu及 thu_rate率--------------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期四' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_thu = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_thu[is.na(train$received_thu) == T] <- 0
train$thu_rate <- round(train$received_thu / train$get_buy_off,digits = 3)
train$thu_rate[is.na(train$thu_rate) == T] <- 0.000
#---------客户周五领卷并购买的次数（线下）   received_fri 及 fri_rate率-------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期五' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_fri = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_fri[is.na(train$received_fri) == T] <- 0
train$fri_rate <- round(train$received_fri / train$get_buy_off,digits = 3)
train$fri_rate[is.na(train$fri_rate) == T] <- 0.000
#---------客户周六领卷并购买的次数（线下）   received_sat 及 sat_rate率-------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期六' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_sat = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_sat[is.na(train$received_sat) == T] <- 0
train$sat_rate <- round(train$received_sat / train$get_buy_off,digits = 3)
train$sat_rate[is.na(train$sat_rate) == T] <- 0.000
#---------客户周日领卷并购买的次数（线下）   received_sun 及 sun_rate率-------------
midd <- train_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期日' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_sun = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$received_sun[is.na(train$received_sun) == T] <- 0
train$sun_rate <- round(train$received_sun / train$get_buy_off,digits = 3)
train$sun_rate[is.na(train$sun_rate) == T] <- 0.000
#-----------客户工作日领取优惠卷的次数--received_workday -------------
train$received_workday <- train$received_mon + train$received_tur + train$received_wen + 
  train$received_thu + train$received_fri
#--------------客户周末领取优惠卷的次数	received_weekend 及 weekenf_rate率- --------
train$received_weekend <- train$received_sat + train$received_sun
train$weekenf_rate <- round(train$received_weekend / train$get_buy_off,digits = 3)
train$weekenf_rate[is.na(train$weekenf_rate) == T] <- 0.000
################### 与距离相关的 ############################
#-------线下购买最长最短，平均距离  ----------------
attr(train_offline$Distance,'levels')
train_offline$Distance <- ordered(train_offline$Distance,
                                  levels = c('0','1','2','3','4','5','6','7','8','9','10','null'))
train_offline$Distance <- as.character(train_offline$Distance)
train_offline$Distance[train_offline$Distance == 'null'] <- NA
train_offline$Distance <- as.numeric(train_offline$Distance)   #直接转会因排序问题引起数据乱，故先排序再字符再数值
midd <- train_offline%>%select(User_id,Distance)%>%group_by(User_id)%>%
  summarise(max_distance = max(Distance,na.rm = T),min_distance = min(Distance,na.rm = T),
            avg_Dis = mean(Distance,na.rm = T))
midd$max_distance[midd$max_distance == -Inf] <- NA
midd$min_distance[midd$min_distance == Inf] <- NA
midd$avg_Dis <- round(midd$avg_Dis,digits = 2)
train <- merge(train,midd,by = 'User_id',all.x = T)
median(train$max_distance,na.rm = T)  # 最长距离的中位数 1
train$max_distance[is.na(train$max_distance) == T] <- 1
median(train$min_distance,na.rm = T)  # 最短距离的中位数 0
train$min_distance[is.na(train$min_distance) == T] <- 0
median(train$avg_Dis,na.rm = T)    #平均距离的中位数是1
train$avg_Dis[is.na(train$avg_Dis) == T] <- 0.8
#--- 不同距离供应商所发的卷  被领取的总次数 ---- 
midd <- train_offline%>%select(Distance_copy,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Distance_copy)%>%summarise(Distance_get_count = n())
train <- merge(train,midd,by = 'Distance_copy',all.x = T)
#----不同距离供应商所发的卷  领取未购买的次数 Distance_get_no_buy --------
midd <- train_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_no_buy = n())
train <- merge(train,midd,by = 'Distance_copy',all.x = T)
#----不同距离供应商所发的卷  被购买的总次数 Distance_get_buy-----------------
midd <- train_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_buy = n())
train <- merge(train,midd,by = 'Distance_copy',all.x = T)
#-----不同距离供应商所发的卷  在15日内购买的次数 Distance_get_buy15----------
midd <- train_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_buy15 = n())
train <- merge(train,midd,by = 'Distance_copy',all.x = T)
#------不同距离供应商所发的卷  在15日内购买率 Distance_get_buy15_rate----------
train$Distance_get_buy15_rate <- round(train$Distance_get_buy15 / train$Distance_get_count,digits = 3)
gc()
######################## 与供应商相关 ###############################
#--- 供应商被领取优惠卷的总次数 ----    
midd <- train_offline%>%select(Merchant_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Merchant_id)%>%summarise(M_get_count_off=n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_count_off[is.na(train$M_get_count_off) == T] <- 0
#----供应商被领取优惠卷未购买的次数  M_get_no_buy_off--------
midd <- train_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Merchant_id)%>%summarise(M_get_no_buy_off=n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_no_buy_off[is.na(train$M_get_no_buy_off) == T] <- 0
#----供应商被领取优惠卷并且已经购买的次数 M_get_buy_off-----------------
midd <- train_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Merchant_id)%>%summarise(M_get_buy_off=n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_buy_off[is.na(train$M_get_buy_off) == T] <- 0
#-------------供应商什么卷也不用的普通购买次 M_nodiscount_buy_off--------
midd <- train_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(Merchant_id)%>%summarise(M_nodiscount_buy_off = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_nodiscount_buy_off[is.na(train$M_nodiscount_buy_off) == T] <- 0
#------供应商领取优惠卷并且在15日内购买的次数 M_get_buy_off15----------
midd <- train_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Merchant_id)%>%summarise(M_get_buy_off15=n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_buy_off15[is.na(train$M_get_buy_off15) == T] <- 0
#------供应商领取优惠卷并购买的次数  没在15日内  M_get_buy_off_not15----------
train$M_get_buy_off_not15 <- train$M_get_buy_off - train$M_get_buy_off15
#------------供应商领取满减卷的次数  M_get_mj_off---------------
midd <- train_offline%>%select(Merchant_id,Discount_fac)%>%filter(Discount_fac == 1)%>%
  group_by(Merchant_id)%>%summarise(M_get_mj_off = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_mj_off[is.na(train$M_get_mj_off) == T] <- 0
#-----------供应商领取折扣卷的次数  M_get_zk_off---------------------
midd <- train_offline%>%select(Merchant_id,Discount_fac)%>%filter(Discount_fac == 0)%>%
  group_by(Merchant_id)%>%summarise(M_get_zk_off = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_get_zk_off[is.na(train$M_get_zk_off) == T] <- 0
#----------------供应商使用满减卷购买的总次数  M_mj_buy_off-------------------------
midd <- train_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 1 )%>%group_by(Merchant_id)%>%summarise( M_mj_buy_off = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_mj_buy_off[is.na(train$M_mj_buy_off) == T] <- 0
#----------------供应商使用满减卷并在15日内购买的次数   M_mj_buy_off15-------------------------
midd <- train_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 1 )%>%group_by(Merchant_id)%>%summarise( M_mj_buy_off15 = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_mj_buy_off15[is.na(train$M_mj_buy_off15) == T] <- 0
#----------------供应商使用折扣卷购买的总次数 M_zk_buy_off---------------
midd <- train_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 0 )%>%group_by(Merchant_id)%>%summarise( M_zk_buy_off = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_zk_buy_off[is.na(train$M_zk_buy_off) == T] <- 0
#----------------供应商使用折扣卷并在15日内购买的总次数  M_zk_buy_off15---------------
midd <- train_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 0 )%>%group_by(Merchant_id)%>%summarise( M_zk_buy_off15 = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$M_zk_buy_off15[is.na(train$M_zk_buy_off15) == T] <- 0
#-----------供应商 满减卷正样本 占比 M_mj_positive_rate------------------
train$M_mj_positive_rate <- round(train$M_mj_buy_off15 / train$M_get_mj_off,digits = 3)
train$M_mj_positive_rate[is.na(train$M_mj_positive_rate) == T] <- 0.000
#-----------供应商 折扣卷正样本 占比 M_zk_positive_rate------------------
train$M_zk_positive_rate <- round(train$M_zk_buy_off15 / train$M_get_zk_off,digits = 3)
train$M_zk_positive_rate[is.na(train$M_zk_positive_rate) == T] <- 0.000
#-----------供应商 无卷购买占比 nodiscount_rate-------------------
train$M_nodiscount_rate <- round(train$M_nodiscount_buy_off / 
                                   (train$M_get_buy_off + train$M_nodiscount_buy_off),digits = 3)
train$M_nodiscount_rate[is.na(train$M_nodiscount_rate) == T] <- 0.000
#-----------供应商 用卷正样本 占比 get_positive_rate-------------------
train$M_get_positive_rate <- round(train$M_get_buy_off15 / train$M_get_count_off,digits = 3)
train$M_get_positive_rate[is.na(train$M_get_positive_rate) == T] <- 0.000
#-----------供应商 用卷负样本样本（领卷已购买但不在15天之内及领了就没买） 占比 get_negative_rate ------
train$M_get_negative_rate <- round( (train$M_get_buy_off_not15 + train$M_get_no_buy_off) / 
                                      train$M_get_count_off,digits = 3)
train$M_get_negative_rate[is.na(train$M_get_negative_rate) == T] <- 0.000
#-----------供应商 用卷15日后购买 占比 get_negative_15rate ------
train$M_get_negative_15rate <- round( train$M_get_buy_off_not15 / train$M_get_count_off,digits = 3)
train$M_get_negative_15rate[is.na(train$M_get_negative_15rate) == T] <- 0.000
#----------供应商 正样本 与 总购买次数的占比 buy_positive_rate------------------
train$M_buy_positive_rate <- round(train$M_get_buy_off15 /
                                     (train$M_nodiscount_buy_off + train$M_get_buy_off),digits = 3)
train$M_buy_positive_rate[is.na(train$M_buy_positive_rate) == T] <- 0.000
gc()
##################### 与折扣相关 ############################
#--- 每种折扣被领取的总次数 ----    
midd <- train_offline%>%select(Discount_rate,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Discount_rate)%>%summarise(rate_get_count = n())
train <- merge(train,midd,by = 'Discount_rate',all.x = T)
train$rate_get_count[is.na(train$rate_get_count) == T] <- 0
#----每种折扣领取未购买的次数 rate_get_no_buy --------
midd <- train_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Discount_rate)%>%summarise(rate_get_no_buy = n())
train <- merge(train,midd,by = 'Discount_rate',all.x = T)
train$rate_get_no_buy[is.na(train$rate_get_no_buy) == T] <- 0
#----每种折扣被购买的总次数 rate_get_buy-----------------
midd <- train_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Discount_rate)%>%summarise(rate_get_buy = n())
train <- merge(train,midd,by = 'Discount_rate',all.x = T)
train$rate_get_buy[is.na(train$rate_get_buy) == T] <- 0
#------每种折扣在15日内购买的次数 rate_get_buy15----------
midd <- train_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Discount_rate)%>%summarise(rate_get_buy15 = n())
train <- merge(train,midd,by = 'Discount_rate',all.x = T)
train$rate_get_buy15[is.na(train$rate_get_buy15) == T] <- 0
#------每种折扣在15日内购买率 rate_get_buy15_rate----------
train$rate_get_buy15_rate <- round(train$rate_get_buy15 / train$rate_get_count,digits = 3)
train$rate_get_buy15_rate[is.na(train$rate_get_buy15_rate) == T] <- 0.000
############ 与预测集 User_id与Merchant,Discount 相关 判断类变量 ######################
#------客户所领取的卷 是不是 TOP10的优惠卷  is_top10 ----------
midd <- sort(unique(train$rate_get_buy15_rate),decreasing = T)
midd <- midd[1:10]
train$is_top10 <- ifelse(train$rate_get_buy15_rate %in% midd,1,0)
##------客户所领取的卷 是不是 首次发行的  is_new_discount_rate ----------
train$is_new_discount_rate <- ifelse(train$rate_get_count == 0,1,0)
#-------------预测集中领取优惠卷的客户是否在已购供应商范围之内   M_is_in------------------------
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac != 2)%>%group_by(User_id)
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Merchant_id)%>%group_by(User_id)%>%
  summarise(items = paste(Merchant_id, collapse=',') )   #先去重重值再生成已购（含普通购买）供应商向量
train <- merge(train,midd,by = 'User_id',all.x = T)
#这里贴过来的items中有NA，因为有的客户以前集中就没见好就购买过，或是本月新客户
grepFun <- function(train){
  grepl(train['Merchant_id'],train['items'],fixed=TRUE)
}                                                #grepl不能向量化，只能定义个函数再APPLY
train$M_is_in <- apply(train,1,grepFun)
train$M_is_in[train$M_is_in == TRUE] <- 1
train$M_is_in[train$M_is_in == FALSE] <- 0
train <- subset(train,select = -c(items))       #删除列 items
#----------预测集中领取优惠卷的客户是否在已用优惠卷购买过的供应商范围之内(不含普通购买和超15日购买) M_is_in_getbuy---
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Merchant_id)%>%group_by(User_id)%>%
  summarise(items = paste(Merchant_id, collapse=',') )   #先去重重值再生成已购（含普通购买）供应商向量
train <- merge(train,midd,by = 'User_id',all.x = T)
#这里贴过来的items中有NA，因为有的客户以前集中就没见好就购买过，或是本月新客户
grepFun <- function(train){
  grepl(train['Merchant_id'],train['items'],fixed=TRUE)
}                                                #grepl不能向量化，只能定义个函数再APPLY
train$M_is_in_getbuy <- apply(train,1,grepFun)
train$M_is_in_getbuy[train$M_is_in_getbuy == TRUE] <- 1
train$M_is_in_getbuy[train$M_is_in_getbuy == FALSE] <- 0
train <- subset(train,select = -c(items))       #删除列 items
#---------客户所领的卷的折扣 ，是否之前也在线下购买过  Discount_is_buy_off--------------------
midd <- train_offline%>%select(User_id,Discount_rate,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)  
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Discount_rate)%>%group_by(User_id)%>%
  summarise(items = paste(Discount_rate, collapse=',') )   
train <- merge(train,midd,by = 'User_id',all.x = T)
grepFun <- function(train){
  grepl(train['Discount_rate'],train['items'],fixed=TRUE)
}                                                
train$Discount_is_buy_off <- apply(train,1,grepFun)
train$Discount_is_buy_off[train$Discount_is_buy_off == TRUE] <- 1
train$Discount_is_buy_off[train$Discount_is_buy_off == FALSE] <- 0
train <- subset(train,select = -c(items))      
#-------------------------此客户是否是第一次领取优惠卷 is_first------------------
midd_1 <- train_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)
midd_2 <- train%>%select(User_id,Date_received)
midd <- rbind(midd_1,midd_2)
midd <- midd%>%select(User_id,Date_received)%>%group_by(User_id)%>%
  summarise(min = min(Date_received,na.rm = T))
train <- merge(train,midd,by = 'User_id',all.x = T)  
train$is_first <- 0
train$is_first[(train$min >= train$Date_received) == T] <- 1
train <- subset(train,select = -c(min))
#-------------客户之前是否也领取过该号码的特殊优惠卷 is_get_Coupon-----------
midd <- train_offline%>%select(User_id,Coupon_id,Date_received)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Coupon_id, .keep_all = TRUE)   #distinct的多列去重
midd <- midd%>%select(User_id,Coupon_id)%>%group_by(User_id)%>%
  summarise(items = paste(Coupon_id, collapse=',') )
train <- merge(train,midd,by = 'User_id',all.x = T)
grepFun <- function(train){
  grepl(train['Coupon_id'],train['items'],fixed=TRUE)
}                                                
train$is_get_Coupon <- apply(train,1,grepFun)
train$is_get_Coupon[train$is_get_Coupon == TRUE] <- 1
train$is_get_Coupon[train$is_get_Coupon == FALSE] <- 0
train <- subset(train,select = -c(items))   

#------------客户之前是否购买过该号码的特殊优惠卷  is_buy_Coupon-----------
midd <- train_offline%>%select(User_id,Coupon_id,buy_fac)%>%
  filter(buy_fac == 1 |buy_fac == 0 )%>%group_by(User_id)  
midd <- midd%>%select(User_id,Coupon_id)%>%group_by(User_id)%>%
  summarise(items = paste(Coupon_id, collapse=',') )
train <- merge(train,midd,by = 'User_id',all.x = T)
grepFun <- function(train){
  grepl(train['Coupon_id'],train['items'],fixed=TRUE)
}                                                
train$is_buy_Coupon <- apply(train,1,grepFun)
train$is_buy_Coupon[train$is_buy_Coupon == TRUE] <- 1
train$is_buy_Coupon[train$is_buy_Coupon == FALSE] <- 0
train <- subset(train,select = -c(items))   
######################### 预测集中 客户与特殊号码卷的交互变量 ##################################
#----------预测集中的客户 领取该号码优惠卷 的总次数 U_C_getcount---
midd <- train_offline%>%select(User_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_getcount = n())  
train <- merge(train,midd,by = c('User_id','Coupon_id'),all.x = T)  
train$U_C_getcount[is.na(train$U_C_getcount) == T] <- 0
#----预测集中的客户 领取该号码优惠卷 并且已购买的次数 U_C_buycount -----------
midd <- train_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_buycount = n())  
train <- merge(train,midd,by = c('User_id','Coupon_id'),all.x = T) 
train$U_C_buycount[is.na(train$U_C_buycount) == T] <- 0
#----预测集中的客户 领取该号码优惠卷 并且在15日内购买的次数 U_C_buy_count15---
midd <- train_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_buy_count15 = n())  
train <- merge(train,midd,by = c('User_id','Coupon_id'),all.x = T) 
train$U_C_buy_count15[is.na(train$U_C_buy_count15) == T] <- 0
#---预测集中的客户 领取该号码优惠卷 但并未购买的次数 U_C_get_no_buy_count-----
midd <- train_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_get_no_buy_count = n())  
train <- merge(train,midd,by = c('User_id','Coupon_id'),all.x = T) 
train$U_C_get_no_buy_count[is.na(train$U_C_get_no_buy_count) == T] <- 0
#----预测集中的客户 在该号码上的 正样本率 U_C_getbuy_rate15-------
train$U_C_getbuy_rate15 <- round(train$U_C_buy_count15 / train$U_C_getcount,digits = 3)
train$U_C_getbuy_rate15[is.na(train$U_C_getbuy_rate15) == T] <- 0
#------预测集中的客户历史上领取过多少个不同的特殊号码U_Couponid_count------
midd <- train_offline%>%select(User_id,Coupon_id,Date_received)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Coupon_id, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Couponid_count = n())
train <- merge(train,midd_1,by = 'User_id',all.x = T) 
train$U_Couponid_count[is.na(train$U_Couponid_count) == T] <- 0
#-----预测集中的客户在该号码上正样本占比 U_C_positive_rate-----
#该号码的正样本数量/历史上总的正样本数量
train$U_C_positive_rate <-  round(train$U_C_buy_count15 / train$Coupon_get_buy_15,digits = 3)
train$U_C_positive_rate[is.na(train$U_C_positive_rate) == T] <- 0
#-----预测集中的客户在该号码上负样本占比 U_C_nagtive_rate----
#该号码的负样本数量/历史上总的负样本数量
train$U_C_nagtive_rate <-  round(train$U_C_get_no_buy_count / train$Coupon_get_no_buy_count,digits = 3)
train$U_C_nagtive_rate[is.na(train$U_C_nagtive_rate) == T] <- 0

######################### 预测集中 客户与供应商交互变量 ##################################
#---预测集中的客户在该供应商 领取的总次数 U_M_getcount-----------------
midd <- train_offline%>%select(User_id,Merchant_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_getcount = n())  
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T)  
train$U_M_getcount[is.na(train$U_M_getcount) == T] <- 0
#---预测集中的客户在该供应商 领取并购买的总次数 U_M_get_buy_count-----------------
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_get_buy_count = n())  
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T) 
train$U_M_get_buy_count[is.na(train$U_M_get_buy_count) == T] <- 0
#---预测集中的客户在该供应商 无卷购买的总次数 U_M_nodiscount-----------------
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_nodiscount = n())   
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
train$U_M_nodiscount[is.na(train$U_M_nodiscount) == T] <- 0
#---预测集中的客户在该供应商 领卷无购买的总次数 U_M_get_no_buy_count-----------------
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_get_no_buy_count = n())   
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
train$U_M_get_no_buy_count[is.na(train$U_M_get_no_buy_count) == T] <- 0
#---预测集中的客户在该供应商 领取并在15日内购买次数 U_M_buycount15-----------------
midd <- train_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_buycount15 = n())   #同一客户同一供应商，购买次数
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
train$U_M_buycount15[is.na(train$U_M_buycount15) == T] <- 0
#---预测集中的客户在该供应商 正样本率（领取并在15日内购买占比） U_M_get_buy_rate15-----------------
train$U_M_get_buy_rate15 <- round(train$U_M_buycount15 / train$U_M_getcount,digits = 3)
train$U_M_get_buy_rate15[is.na(train$U_M_get_buy_rate15) == T] <- 0
#---预测集中的客户历史上领取过优惠卷的有多少个不同的供应商 U_Merchant_id_count----
midd <- train_offline%>%select(User_id,Merchant_id,Coupon_id)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Merchant_id, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Merchant_id_count = n())
train <- merge(train,midd_1,by = 'User_id',all.x = T) 
train$U_Merchant_id_count[is.na(train$U_Merchant_id_count) == T] <- 0
#------------预测集中的客户在该供应商正样本占比 U_M_positive_rate-------------
#该供应的正样本数量/历史上的供应商中总的正样本数量
train$U_M_positive_rate = round(train$U_M_get_buy_rate15 / train$get_buy_off15,digits = 3)
train$U_M_positive_rate[is.na(train$U_M_positive_rate) == T] <- 0
#------------预测集中的客户在该供应商负样本占比  U_M_nagtive_rate-------------
#该供应的负样本数量/历史上的供应商中总的负样本数量）
train$U_M_nagtive_rate = round(train$U_M_get_no_buy_count / train$get_no_buy_off,digits = 3)
train$U_M_nagtive_rate[is.na(train$U_M_nagtive_rate) == T] <- 0
#-----对比供应商平均正样本率的变化量 U_M_avg_pos_incremental ---------
#预测集中的客户在该供应商正样本占比 - 正样本率供应商平均值
train$U_M_avg_pos_incremental <- train$U_M_positive_rate - 
  train$get_buy_off15/train$get_count_off/train$U_Merchant_id_count
train$U_M_avg_pos_incremental <- round(train$U_M_avg_pos_incremental,digits = 3)
train$U_M_avg_pos_incremental[is.na(train$U_M_avg_pos_incremental) == T] <- 0
#-----对比供应商平均负样本率的变化量 U_M_avg_nag_incremental ---------
#预测集中的客户在该供应商负样本占比 - 负样本率供应商平均值
train$U_M_avg_nag_incremental <- train$U_M_nagtive_rate - 
  (train$get_buy_off_not15 + train$get_no_buy_off)/train$get_count_off/train$U_Merchant_id_count
train$U_M_avg_nag_incremental <- round(train$U_M_avg_nag_incremental,digits = 3)
train$U_M_avg_nag_incremental[is.na(train$U_M_avg_nag_incremental) == T] <- 0
######################### 预测集中 客户与折扣率交互变量 ##################################
#---预测集中的客户所领卷的折扣率  历史领取的总次数 U_D_getcount-----------------
midd <- train_offline%>%select(User_id,Discount_rate,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_getcount = n())  
train <- merge(train,midd,by = c('User_id','Discount_rate'),all.x = T)  
train$U_D_getcount[is.na(train$U_D_getcount) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领取并购买的总次数 U_D_get_buy_count-----------------
midd <- train_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_get_buy_count = n())  
train <- merge(train,midd,by = c('User_id','Discount_rate'),all.x = T) 
train$U_D_get_buy_count[is.na(train$U_D_get_buy_count) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领卷无购买的总次数 U_D_get_no_buy_count-----------------
midd <- train_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_get_no_buy_count = n())   
train <- merge(train,midd,by = c('User_id','Discount_rate'),all.x = T)  #两个关健字的MERGE
train$U_D_get_no_buy_count[is.na(train$U_D_get_no_buy_count) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领取并在15日内购买次数 U_D_buycount15-----------------
midd <- train_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_buycount15 = n())   #同一客户同一供应商，购买次数
train <- merge(train,midd,by = c('User_id','Discount_rate'),all.x = T)  #两个关健字的MERGE
train$U_D_buycount15[is.na(train$U_D_buycount15) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史 正样本率（领取并在15日内购买占比） U_D_get_buy_rate15-----------------
train$U_D_get_buy_rate15 <- round(train$U_D_buycount15 / train$U_D_getcount,digits = 3)
train$U_D_get_buy_rate15[is.na(train$U_D_get_buy_rate15) == T] <- 0
#-----------预测集中的客户历史上领取过多少个不同折扣U_Discountid_count-----
midd <- train_offline%>%select(User_id,Discount_rate)%>%
  filter(Discount_rate != "null")%>%group_by(User_id)  
midd <- distinct(midd, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Discountid_count = n())
train <- merge(train,midd_1,by = 'User_id',all.x = T) 
train$U_Discountid_count[is.na(train$U_Discountid_count) == T] <- 0
#------预测集中的客户在该折扣上正样本占比--U_D_positive_rate--------------
#（ 该折扣上的正样本数量/历史上总的正样本数量）
train$U_D_positive_rate <- round(train$U_D_buycount15 / train$get_buy_off15,digits = 3)
train$U_D_positive_rate[is.na(train$U_D_positive_rate) == T] <- 0
#--------预测集中的客户在该折扣上负样本占比--U_D_nagtive_rate------------
#（ 该折扣上的负样本数量/历史上总的负样本数量）
train$U_D_nagtive_rate = round(train$U_D_get_no_buy_count / train$get_no_buy_off,digits = 3)
train$U_D_nagtive_rate[is.na(train$U_D_nagtive_rate) == T] <- 0
#-----对比 折扣率 平均正样本率的变化量 U_D_avg_pos_incremental ---------
#预测集中的客户在该折扣率正样本占比 - 正样本率折扣率平均值
train$U_D_avg_pos_incremental <- train$U_D_positive_rate - 
  train$get_buy_off15/train$get_count_off/train$U_Discountid_count
train$U_D_avg_pos_incremental <- round(train$U_D_avg_pos_incremental,digits = 3)
train$U_D_avg_pos_incremental[is.na(train$U_D_avg_pos_incremental) == T] <- 0
#-----对比 折扣率 平均负样本率的变化量 U_D_avg_nag_incremental ---------
#预测集中的客户在该折扣率负样本占比 - 负样本率折扣平均值
train$U_D_avg_nag_incremental <- train$U_D_nagtive_rate - 
  (train$get_buy_off_not15 + train$get_no_buy_off)/train$get_count_off/
  train$U_Discountid_count
train$U_D_avg_nag_incremental <- round(train$U_D_avg_nag_incremental,digits = 3)
train$U_D_avg_nag_incremental[is.na(train$U_D_avg_nag_incremental) == T] <- 0
gc()
######################## 重编码 ######################
#----------------供应商重编码     Merchant_woe---------------------------------------
freq_M <- data.frame(table(train$Merchant_id))
train$Merchant <- train$Merchant_id
train <- merge(train,freq_M,by.x ='Merchant_id',by.y = 'Var1',all.x = T)

freq_M_1 <- data.frame(table(train$Merchant_id,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
woe <- cast(freq_M_1,kind~type)
woe$freq <- woe$group0 + woe$group1
woe <- woe[woe$group0 == 0 | woe$group1 ==0,]
uni <- unique(woe$freq)
uni <- sort(uni,decreasing = TRUE)
rm(woe,freq_M_1)
Merchant_woe_bin <- data.frame()
for (i in 1:length(uni)){
  Merchant_woe_bin[i,1] = uni[i]
  train$Merchant[train$Freq <= uni[i]] <- "other"
  freq_M_1 <- data.frame(table(train$Merchant,train$y))
  names(freq_M_1) <- c("kind","type","freq") 
  freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
  woe = cast(freq_M_1,kind~type)
  woe$group1[woe$group1 == 0] = 0.0001
  woe$group0[woe$group0 == 0] = 0.0001 #存在新发的号码全买的情况
  woe$sum_0 = sum(woe$group0)  
  woe$sum_1 = sum(woe$group1)
  woe$sum_0_p = woe$group0 /woe$sum_0 
  woe$sum_1_p = woe$group1/woe$sum_1
  woe$woe = log1p(woe$sum_1_p/woe$sum_0_p) 
  woe <- woe[-c(2:7)]
  Iv = subset(train,select = c(y,Merchant))
  Iv = merge(Iv,woe,by.x = 'Merchant',by.y = 'kind',all.x = T)
  Iv = Iv[-1]
  Iv$y = as.numeric(as.character(Iv$y))
  sumivt = smbinning.sumiv(df = Iv ,y = "y")
  Merchant_woe_bin[i,2] = sumivt[1,2]
  Merchant_woe_bin[i,3] = uni[i]
  train$Merchant = train$Merchant_id
  print(i)
}
uni <- max(Merchant_woe_bin$V2)
uni <- Merchant_woe_bin[Merchant_woe_bin$V2 == uni,]
uni <- max(uni$V1)
train$Merchant <- train$Merchant_id
train$Merchant[train$Freq <= uni] <- "other"
freq_M_1 <- data.frame(table(train$Merchant,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
Merchant_woe <- cast(freq_M_1,kind~type)
Merchant_woe$group1[Merchant_woe$group1 == 0] = 0.0001
Merchant_woe$group0[Merchant_woe$group0 == 0] = 0.0001
Merchant_woe$sum_0 <- sum(Merchant_woe$group0)  
Merchant_woe$sum_1 <- sum(Merchant_woe$group1)
Merchant_woe$sum_0_p <- Merchant_woe$group0 /Merchant_woe$sum_0 
Merchant_woe$sum_1_p <- Merchant_woe$group1/Merchant_woe$sum_1
Merchant_woe$Merchant_woe <- log1p(Merchant_woe$sum_1_p/Merchant_woe$sum_0_p) 
Merchant_woe <- Merchant_woe[-c(2:7)]
train <- merge(train,Merchant_woe,by.x = 'Merchant',by.y = 'kind',all.x = T)
train <- subset(train,select = -c(Merchant,Freq))
rm(Merchant_woe_bin,freq_M,freq_M_1,Iv,sumivt,i,uni,grepFun)
#-----------------------折扣率重编码   Discount_woe-------------------------------
freq_D <- data.frame(table(train$Discount_rate))
train$Discount <- train$Discount_rate
train <- merge(train,freq_D,by.x ='Discount_rate',by.y = 'Var1',all.x = T)
freq_M_1 <- data.frame(table(train$Discount_rate,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
woe <- cast(freq_M_1,kind~type)
woe$freq <- woe$group0 + woe$group1
woe <- woe[woe$group0 == 0 | woe$group1 ==0,]
uni <- unique(woe$freq)
uni <- sort(uni,decreasing = TRUE)
Discount_woe_bin <- data.frame()
for (i in 1:length(uni)){
  Discount_woe_bin[i,1] = uni[i]
  train$Discount[train$Freq <= uni[i]] <- "other"
  freq_M_1 <- data.frame(table(train$Discount,train$y))
  names(freq_M_1) <- c("kind","type","freq") 
  freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
  woe = cast(freq_M_1,kind~type)
  woe$group1[woe$group1 == 0] = 0.0001
  woe$group0[woe$group0 == 0] = 0.0001 #存在新发的号码全买的情况
  woe$sum_0 = sum(woe$group0)  
  woe$sum_1 = sum(woe$group1)
  woe$sum_0_p = woe$group0 /woe$sum_0 
  woe$sum_1_p = woe$group1/woe$sum_1
  woe$woe = log1p(woe$sum_1_p/woe$sum_0_p) 
  woe <- woe[-c(2:7)]
  Iv = subset(train,select = c(y,Discount))
  Iv = merge(Iv,woe,by.x = 'Discount',by.y = 'kind',all.x = T)
  Iv = Iv[-1]
  Iv$y = as.numeric(as.character(Iv$y))
  sumivt = smbinning.sumiv(df = Iv ,y = "y")
  Discount_woe_bin[i,2] = sumivt[1,2]
  Discount_woe_bin[i,3] = uni[i]
  train$Discount = train$Discount_rate
  print(i)
}
uni <- max(Discount_woe_bin$V2)
uni <- Discount_woe_bin[Discount_woe_bin$V2 == uni,]
uni <- max(uni$V1)
train$Discount <- train$Discount_rate
train$Discount[train$Freq <= uni] <- "other"
freq_M_1 <- data.frame(table(train$Discount,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
Discount_woe <- cast(freq_M_1,kind~type)
Discount_woe$group1[Discount_woe$group1 == 0] = 0.0001
Discount_woe$group0[Discount_woe$group0 == 0] = 0.0001
Discount_woe$sum_0 <- sum(Discount_woe$group0)  
Discount_woe$sum_1 <- sum(Discount_woe$group1)
Discount_woe$sum_0_p <- Discount_woe$group0 /Discount_woe$sum_0 
Discount_woe$sum_1_p <- Discount_woe$group1/Discount_woe$sum_1
Discount_woe$Discount_woe <- log1p(Discount_woe$sum_1_p/Discount_woe$sum_0_p) 
Discount_woe <- Discount_woe[-c(2:7)]
train <- merge(train,Discount_woe,by.x = 'Discount',by.y = 'kind',all.x = T)
train <- subset(train,select = -c(Discount,Freq))
rm(Discount_woe_bin,freq_D,freq_M_1,Iv,sumivt,i,uni,woe)
#-------------距离的重编码---Distance_woe-----------------------
freq_D <- data.frame(table(train$Distance_copy))
train <- merge(train,freq_D,by.x ='Distance_copy',by.y = 'Var1',all.x = T)
freq_D_1 <- data.frame(table(train$Distance_copy,train$y))
names(freq_D_1) <- c("kind","type","freq") 
freq_D_1$type <- paste("group",freq_D_1$type,sep="") 
Distance_woe <- cast(freq_D_1,kind~type)
Distance_woe$sum_0 <- sum(Distance_woe$group0)  #计算0的合计数
Distance_woe$sum_1 <- sum(Distance_woe$group1)
Distance_woe$sum_0_p <- Distance_woe$group0 /Distance_woe$sum_0  #计算品牌占总0的百分比，即概率
Distance_woe$sum_1_p <- Distance_woe$group1/Distance_woe$sum_1
Distance_woe$Distance_woe <- log(Distance_woe$sum_1_p/Distance_woe$sum_0_p)  # 计算woe值
Distance_woe <- Distance_woe[-c(2:7)]
train <- merge(train,Distance_woe,by.x = 'Distance_copy',by.y = 'kind',all.x = T)
train <- subset(train,select = -c(Distance_copy,Freq))
rm(freq_D,freq_D_1)
#-------------领取优惠卷是星期几的重编码---Weekday_woe-----------------------
freq_D <- data.frame(table(train$is_weekday))
train <- merge(train,freq_D,by.x ='is_weekday',by.y = 'Var1',all.x = T)
freq_D_1 <- data.frame(table(train$is_weekday,train$y))
names(freq_D_1) <- c("kind","type","freq") 
freq_D_1$type <- paste("group",freq_D_1$type,sep="") 
Weekday_woe <- cast(freq_D_1,kind~type)
Weekday_woe$sum_0 <- sum(Weekday_woe$group0)  #计算0的合计数
Weekday_woe$sum_1 <- sum(Weekday_woe$group1)
Weekday_woe$sum_0_p <- Weekday_woe$group0 /Weekday_woe$sum_0  #计算品牌占总0的百分比，即概率
Weekday_woe$sum_1_p <- Weekday_woe$group1/Weekday_woe$sum_1
Weekday_woe$Weekday_woe <- log(Weekday_woe$sum_1_p/Weekday_woe$sum_0_p)  # 计算woe值
Weekday_woe <- Weekday_woe[-c(2:7)]
train <- merge(train,Weekday_woe,by.x = 'is_weekday',by.y = 'kind',all.x = T)
train <- subset(train,select = -c(Freq))
rm(freq_D,freq_D_1)
#-----------特殊号码优惠卷的重编码  Coupon_woe---------------
freq_M <- data.frame(table(train$Coupon_id))
train$Coupon <- train$Coupon_id
train <- merge(train,freq_M,by.x ='Coupon_id',by.y = 'Var1',all.x = T)
freq_M_1 <- data.frame(table(train$Coupon_id,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
woe <- cast(freq_M_1,kind~type)
woe$freq <- woe$group0 + woe$group1
woe <- woe[woe$group0 == 0 | woe$group1 ==0,]
uni <- unique(woe$freq)
uni <- sort(uni)
rm(woe,freq_M_1)
Coupon_woe_bin <- data.frame()
for (i in 1:length(uni)){
  Coupon_woe_bin[i,1] = uni[i]
  train$Coupon[train$Freq <= uni[i]] <- "other"
  freq_M_1 <- data.frame(table(train$Coupon,train$y))
  names(freq_M_1) <- c("kind","type","freq") 
  freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
  woe = cast(freq_M_1,kind~type)
  woe$group1[woe$group1 == 0] = 0.0001
  woe$group0[woe$group0 == 0] = 0.0001 #存在新发的号码全买的情况
  woe$sum_0 = sum(woe$group0)  
  woe$sum_1 = sum(woe$group1)
  woe$sum_0_p = woe$group0 /woe$sum_0 
  woe$sum_1_p = woe$group1/woe$sum_1
  woe$woe = log1p(woe$sum_1_p/woe$sum_0_p) 
  woe <- woe[-c(2:7)]
  Iv = subset(train,select = c(y,Coupon))
  Iv = merge(Iv,woe,by.x = 'Coupon',by.y = 'kind',all.x = T)
  Iv = Iv[-1]
  Iv$y = as.numeric(as.character(Iv$y))
  sumivt = smbinning.sumiv(df = Iv ,y = "y")
  Coupon_woe_bin[i,2] = sumivt[1,2]
  Coupon_woe_bin[i,3] = uni[i]
  train$Coupon = train$Coupon_id
  print(i)
}
uni <- max(Coupon_woe_bin$V2)
uni <- Coupon_woe_bin[Coupon_woe_bin$V2 == uni,]
uni <- max(uni$V1)
train$Coupon <- train$Coupon_id
train$Coupon[train$Freq <= uni] <- "other"
freq_M_1 <- data.frame(table(train$Coupon,train$y))
names(freq_M_1) <- c("kind","type","freq") 
freq_M_1$type <- paste("group",freq_M_1$type,sep="") 
Coupon_woe <- cast(freq_M_1,kind~type)
Coupon_woe$group1[Coupon_woe$group1 == 0] = 0.0001
Coupon_woe$group0[Coupon_woe$group0 == 0] = 0.0001
Coupon_woe$sum_0 <- sum(Coupon_woe$group0)  
Coupon_woe$sum_1 <- sum(Coupon_woe$group1)
Coupon_woe$sum_0_p <- Coupon_woe$group0 /Coupon_woe$sum_0 
Coupon_woe$sum_1_p <- Coupon_woe$group1/Coupon_woe$sum_1
Coupon_woe$Coupon_woe <- log1p(Coupon_woe$sum_1_p/Coupon_woe$sum_0_p) 
Coupon_woe <- Coupon_woe[-c(2:7)]
train <- merge(train,Coupon_woe,by.x = 'Coupon',by.y = 'kind',all.x = T)
train <- subset(train,select = -c(Coupon,Freq))
rm(Coupon_woe_bin,freq_M,freq_M_1,Iv,sumivt,i,uni,woe)
######################## 两个前置周期 ###########################
#-------------------------------客户前一个周期（15~30天）领取的次数  Last_cycle_get_count----------
#因提交集6。16~6.30空白， 4-1~4-15
midd <- train_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)%>%
  filter(Date_received >= '2016-04-15' & Date_received <= '2016-04-30')%>%
  group_by(User_id)%>%summarise( Last_cycle_get_count = n()) 
train <- merge(train,midd,by = 'User_id',all.x = T) 
train$Last_cycle_get_count[is.na(train$Last_cycle_get_count) == T] <- 0
#-------------------------------客户前一个周期（15~30天）领取并购买的次数  Last_cycle_get_buy_count----------
midd <- train_offline%>%select(User_id,Date_received,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 2)%>%
  filter(Date_received >= '2016-04-15' & Date_received <= '2016-04-30')%>%
  group_by(User_id)%>%summarise( Last_cycle_get_buy_count = n()) 
train <- merge(train,midd,by = 'User_id',all.x = T) 
train$Last_cycle_get_buy_count[is.na(train$Last_cycle_get_buy_count) == T] <- 0
#-------------------------------客户前二个周期（31~45天）领取的次数  second_cycle_get_count----------
#3-16~3-31
midd <- train_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)%>%
  filter(Date_received >= '2016-04-01' & Date_received <= '2016-04-15')%>%
  group_by(User_id)%>%summarise( second_cycle_get_count = n()) 
train <- merge(train,midd,by = 'User_id',all.x = T) 
train$second_cycle_get_count[is.na(train$second_cycle_get_count) == T] <- 0
#-------------------------------客户前二个周期（31~45天）领取并购买的次数  second_cycle_get_buy_count----------
midd <- train_offline%>%select(User_id,Date_received,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 2)%>%
  filter(Date_received >= '2016-04-01' & Date_received <= '2016-04-15')%>%
  group_by(User_id)%>%summarise( second_cycle_get_buy_count = n()) 
train <- merge(train,midd,by = 'User_id',all.x = T) 
train$second_cycle_get_buy_count[is.na(train$second_cycle_get_buy_count) == T] <- 0
###########################  与间隔天数相关 ###############################
#---------------每一个客户领取优惠卷到使用优惠卷的最小、平均、最大间隔天数	avg_days,min_days,max_days-------
midd <- train_offline%>%select(User_id,Date_received,Date)%>%group_by(User_id)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(User_id,Date_received,Date,diff)%>%group_by(User_id)%>%
  summarise(min_days = min(diff,na.rm = T),avg_days = mean(diff,na.rm = T),max_days = max(diff,na.rm = T))
train <- merge(train,midd_1,by.x = 'User_id',by.y = 'User_id',all.x = T)
train$min_days[is.na(train$min_days) == T] <- 180
train$max_days[is.na(train$max_days) == T] <- 180
train$avg_days[is.na(train$avg_days) == T] <- 180
#----------------每一种折扣的平均领购时间差	cou_avgdays cou_mindays cou_maxdays---------------
midd <- train_offline%>%select(Discount_rate,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Discount_rate,Date_received,Date,diff)%>%group_by(Discount_rate)%>%
  summarise(cou_mindays = min(diff,na.rm = T),cou_avgdays = mean(diff,na.rm = T),
            cou_maxdays = max(diff,na.rm = T))
train <- merge(train,midd_1,by.x = 'Discount_rate',by.y = 'Discount_rate',all.x = T)
train$cou_mindays[is.na(train$cou_mindays) == T] <- 180
train$cou_avgdays[is.na(train$cou_avgdays) == T] <- 180
train$cou_maxdays[is.na(train$cou_maxdays) == T] <- 180
#----------------每一供应商的发的卷的平均领购时间差	M_avgdays M_mindays M_maxdays---------------
midd <- train_offline%>%select(Merchant_id,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Merchant_id,Date_received,Date,diff)%>%group_by(Merchant_id)%>%
  summarise(M_mindays = min(diff,na.rm = T),M_avgdays = mean(diff,na.rm = T),M_maxdays = max(diff,na.rm = T))
train <- merge(train,midd_1,by.x = 'Merchant_id',by.y = 'Merchant_id',all.x = T)
train$M_mindays[is.na(train$M_mindays) == T] <- 180
train$M_avgdays[is.na(train$M_avgdays) == T] <- 180
train$M_maxdays[is.na(train$M_maxdays) == T] <- 180
#------------------每一特殊号码卷的平均领购时间差 -------------
midd <- train_offline%>%select(Coupon_id,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Coupon_id,Date_received,Date,diff)%>%group_by(Coupon_id)%>%
  summarise(coupon_mindays = min(diff,na.rm = T),coupon_avgdays = mean(diff,na.rm = T),
            coupon_maxdays = max(diff,na.rm = T))
train <- merge(train,midd_1,by.x = 'Coupon_id',by.y = 'Coupon_id',all.x = T)
train$coupon_mindays[is.na(train$coupon_mindays) == T] <- 180
train$coupon_avgdays[is.na(train$coupon_avgdays) == T] <- 180
train$coupon_maxdays[is.na(train$coupon_maxdays) == T] <- 180
#---截止该笔优惠卷领取的日期，领卷距最后一次购买间隔天数	buydays_diff1--------
#---截止该笔优惠卷领取的日期，倒数第一次购买距倒数第二次购买的间隔天数	buydays_diff2------
midd <- train_offline%>%select(User_id,Date)%>%filter(is.na(Date) == F)%>%filter(Date <= '2016-04-30')%>%
  arrange(User_id,desc(Date))%>%group_by(User_id)  # arrange(User_id,desc(Date))注意括号
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date, collapse=','))
midd_1$vars <- substring(midd_1$items,1,21)   
midd_1 <- subset(midd_1,select = -c(items))
datename <- c('last_times','last_second')  
midd_2 <- midd_1%>%separate(vars,datename, sep = ',')                                          
midd_2$last_times <-  as.Date(midd_2$last_times,'%Y-%m-%d') 
midd_2$last_second <-  as.Date(midd_2$last_second,'%Y-%m-%d') 
train <- merge(train,midd_2,by = 'User_id',all.x = T)
train$buydays_diff1 <- train$Date_received - train$last_times
train$buydays_diff1[is.na(train$buydays_diff1) == T] <- 180
train$buydays_diff2 <- train$last_times - train$last_second
train$buydays_diff2[is.na(train$buydays_diff2) == T] <- 180
train <- subset(train,select = -c(last_times,last_second))
########################### 现实场景不存在的变量 #################################
#-------------------客户领取优惠卷的当日是否也领过其他优惠卷is_repeatget----repeat_count-------
#全预测日期单独提出来成为一个新集
midd <- train%>%select(User_id,Date_received)%>%group_by(User_id)
#与之前不同，抽train而不是train_offline,
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] <- as.Date(midd[,i+2],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd[,2]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_repeatget','repeat_count')]
midd$repeat_count <- as.numeric(midd$repeat_count)
midd <- midd[-1]
train <- cbind(train,midd)
#-带号码-----客户领取优惠卷的当日是否也领过同一号码的优惠卷is_cou_repeatget----cou_repeat_count-------
midd <- train%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)
#与之前不同，抽train而不是train_offline,
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$cou_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_cou_repeatget','cou_repeat_count')]
midd$cou_repeat_count <- as.numeric(midd$cou_repeat_count)
midd <- midd[-1]
train <- cbind(train,midd)
#-----截止领卷当日的前15天内，该客户是否领过卷(不含当日)      is_get_before15-----------
#-----截止领卷当日的前15天内，该客户是否领几次卷(不含当日) get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过卷(不含当日)      is_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领几次卷(不含当日) get_after_count----------
midd <- subset(train,select = c(User_id,Date_received))
midd_2 <- subset(train,select = c(User_id,Date_received))
number <- max(table(midd$User_id))  
midd <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = 'User_id',all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+2] <- as.Date(midd[,i+2],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_get_before15','get_before15_count','is_get_after','get_after_count')]
midd$get_before15_count <- as.numeric(midd$get_before15_count)
midd$get_after_count <- as.numeric(midd$get_after_count)
midd <- midd[-1]
train <- cbind(train,midd)
#-带号码---截止领卷当日的前15天内，该客户是否领过同号码卷(不含当日)      is_cou_get_before15-----------
#-带号码---截止领卷当日的前15天内，该客户是否领几次同号码卷(不含当日) cou_get_before15_count-----------
#-带号码----截止领卷当日之后到月底之前，该客户是否领过同号码卷(不含当日)      is_cou_get_after----------
#-带号码----截止领卷当日之后到月底之前，该客户是否领几次同号码卷(不含当日) cou_get_after_count----------
midd <- subset(train,select = c(User_id,Coupon_id,Date_received))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(train,select = c(User_id,Coupon_id,Date_received))
midd <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Coupon_id'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$cou_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$cou_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_cou_get_before15','cou_get_before15_count',
               'is_cou_get_after','cou_get_after_count')]
midd$cou_get_before15_count <- as.numeric(midd$cou_get_before15_count)
midd$cou_get_after_count <- as.numeric(midd$cou_get_after_count)
midd <- midd[-1]
train <- cbind(train,midd)
#-------------预测期内客户领卷的次数  P_count ----------------
midd <- train%>%select(User_id,Date_received)%>%group_by(User_id)%>%summarise( P_count = n())
train <- merge(train,midd,by = 'User_id',all.x = T)  
#--带号码-----------预测期内客户领同号码卷的次数  cou_P_count ----------------
midd <- train%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%summarise( cou_P_count = n())
train <- merge(train,midd,by = c('User_id','Coupon_id'),all.x = T) 
#------------------------------预测期内客户领卷距下一次领卷的天数（不含当日）P_afterdays----------------
midd <- train%>%select(User_id,Date_received)%>%group_by(User_id)%>%arrange(User_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Date_received)
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] = as.Date(midd[,i+2],'%Y-%m-%d')
  a = midd[,2] - midd[,i+2]
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2][midd[,i+2] >= 0] = -31
}
midd$P_afterdays <- apply(midd[,3:(number+2)],1,max)
midd$P_afterdays <- abs(midd$P_afterdays)
midd <- midd[-c(2:(number+2))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]

#-带号码----------预测期内客户领卷距下一次领同号码卷的天数（不含当日）cou_P_afterdays----------------
midd <- train%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%arrange(User_id,Coupon_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Coupon_id,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$cou_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$cou_P_afterdays <- abs(midd$cou_P_afterdays)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
#----------------------预测期内客户领卷距上一次领卷的天数（不含当日）P_beforedays-------------------
midd <- train%>%select(User_id,Date_received)%>%group_by(User_id)%>%arrange(User_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Date_received)
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] = as.Date(midd[,i+2],'%Y-%m-%d')
  a = midd[,2] - midd[,i+2]
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2][midd[,i+2] <= 0] = 31
}
midd$P_beforedays <- apply(midd[,3:(number+2)],1,min)
midd <- midd[-c(2:(number+2))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
#--带号码-------预测期内客户领卷距上一次领同号码卷的天数（不含当日）cou_P_beforedays-------------------
midd <- train%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%arrange(User_id,Coupon_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Coupon_id,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$cou_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
################-----------追加------------####################-
#--------------供应商发过几个特殊号的卷---- M_Cou_count----
midd <- train_offline%>%select(Merchant_id,Coupon_id)%>%filter(Coupon_id !='null')%>%
  group_by(Merchant_id,Coupon_id)%>%summarise(n=n())
midd_1 <- midd%>%select(Merchant_id,Coupon_id)%>%group_by(Merchant_id)%>%summarise(M_Cou_count=n())
train <- merge(train,midd_1,by = 'Merchant_id',all.x = T)
train$M_Cou_count[is.na(train$M_Cou_count) == T] <- 0
#-----------预测集的特殊号码卷，点该供商商所发的卷的占比cou_M_rate  ---
midd_1 <- train_offline%>%select(Coupon_id,Date_received)%>%filter(Coupon_id !='null')%>%
  group_by(Coupon_id)%>%summarise(cou=n())
train <- merge(train,midd_1,by = 'Coupon_id',all.x = T)
train$cou[is.na(train$cou)] <- 0
midd <- train_offline%>%select(Merchant_id,Coupon_id,Date_received)%>%filter(Coupon_id !='null')%>%
  group_by(Merchant_id)%>%summarise(n = n())
train <- merge(train,midd,by = 'Merchant_id',all.x = T)
train$n[is.na(train$n)] <- 0
train$cou_M_rate <- train$cou/train$n
train$cou_M_rate[is.na(train$cou_M_rate)] <- 0
train <- subset(train,select = -c(cou,n))
#-------------- 预测期内 同一折扣领卷的次数  p_discount------------
midd <- train%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%summarise( p_discount = n())
train <- merge(train,midd,by = c('User_id','Discount_rate'),all.x = T) 
#----------预测期内客户领卷距下一次领同一折扣的天数（不含当日）discount_P_afterdays----------------
midd <- train%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%arrange(User_id,Discount_rate,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Discount_rate,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$discount_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$discount_P_afterdays <- abs(midd$discount_P_afterdays)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
#--------预测期内客户领卷距上一次领同一折扣的天数（不含当日）discount_P_beforedays-------------------
midd <- train%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%arrange(User_id,Discount_rate,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Discount_rate,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$discount_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
#-----客户领取优惠卷的当日是否也领过同一折扣的优惠卷is_discount_repeatget----discount_repeat_count
midd <- train%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$discount_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_discount_repeatget','discount_repeat_count')]
midd$discount_repeat_count <- as.numeric(midd$discount_repeat_count)
midd <- midd[-1]
train <- cbind(train,midd)

#----截止领卷当日的前15天内，该客户是否领过同一折扣卷(不含当日)      is_discount_get_before15-----------
#----截止领卷当日的前15天内，该客户领过几次同一折扣卷(不含当日) discount_get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一折扣卷(不含当日)      is_discount_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一折扣卷(不含当日) discount_get_after_count----------
midd <- subset(train,select = c(User_id,Discount_rate,Date_received))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(train,select = c(User_id,Discount_rate,Date_received))
midd <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Discount_rate'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$discount_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$discount_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_discount_get_before15','discount_get_before15_count',
               'is_discount_get_after','discount_get_after_count')]
midd$discount_get_before15_count <- as.numeric(midd$discount_get_before15_count)
midd$discount_get_after_count <- as.numeric(midd$discount_get_after_count)
midd <- midd[-1]
train <- cbind(train,midd)
###########-------------------------------#############################

#-------------- 预测期内 同一供应商领卷的次数  p_Mer------------
midd <- train%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%summarise( p_Mer = n())
train <- merge(train,midd,by = c('User_id','Merchant_id'),all.x = T) 
#----------预测期内客户领卷距下一次领同一供应商卷的天数（不含当日）Mer_P_afterdays----------------
midd <- train%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%arrange(User_id,Merchant_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Merchant_id,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$Mer_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$Mer_P_afterdays <- abs(midd$Mer_P_afterdays)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
#--------预测期内客户领卷距上一次领同一供应商的天数（不含当日）Mer_P_beforedays-------------------
midd <- train%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%arrange(User_id,Merchant_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- train%>%select(User_id,Merchant_id,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$Mer_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
train <- cbind(train,midd)
train <- train[-c(ncol(train)-1)]
train <- train[-c(ncol(train)-1)]
#-----客户领取优惠卷的当日是否也领过同一供应商的优惠卷is_Mer_repeatget----Mer_repeat_count
midd <- train%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$Mer_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_Mer_repeatget','Mer_repeat_count')]
midd$Mer_repeat_count <- as.numeric(midd$Mer_repeat_count)
midd <- midd[-1]
train <- cbind(train,midd)

#----截止领卷当日的前15天内，该客户是否领过同一供应商卷(不含当日)      is_Mer_get_before15-----------
#----截止领卷当日的前15天内，该客户领过几次同一供应商卷(不含当日) Mer_get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一供应商卷(不含当日)      is_Mer_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一供应商卷(不含当日) Mer_get_after_count----------
midd <- subset(train,select = c(User_id,Merchant_id,Date_received))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(train,select = c(User_id,Merchant_id,Date_received))
midd <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Merchant_id'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$Mer_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$Mer_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_Mer_get_before15','Mer_get_before15_count',
               'is_Mer_get_after','Mer_get_after_count')]
midd$Mer_get_before15_count <- as.numeric(midd$Mer_get_before15_count)
midd$Mer_get_after_count <- as.numeric(midd$Mer_get_after_count)
midd <- midd[-1]
train <- cbind(train,midd)
write.csv(train,"E:/R 3.4.2 for Windows/O2O_tc/train_two.csv")
#---------------------------- 预测期内客户是第几次领卷  rank ---------------------------
index<-function(x){return(c(1:length(x)))}
train <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/train_two.csv")
train <- train[-1]
train <- sqldf("select * from train order by User_id,Date_received")
train$User_id <- as.factor(train$User_id)
train <-transform(train, group = as.integer(User_id))
train1 <- train[1:50000,]
train1 <- transform(train1,rank = unlist(tapply(Date_received,group,index)))
train2 <- train[50001:100000,]
train2 <- transform(train2,rank = unlist(tapply(Date_received,group,index)))
train3 <- train[100001:150000,]
train3 <- transform(train3,rank = unlist(tapply(Date_received,group,index)))
train4 <- train[150001:200000,]
train4 <- transform(train4,rank = unlist(tapply(Date_received,group,index)))
train5 <- train[200001:252586,]
train5 <- transform(train5,rank = unlist(tapply(Date_received,group,index)))
train <- rbind(train1,train2,train3,train4,train5)
train <- subset(train,select = -c(group))
rm(train1,train2,train3,train4,train5)
rm(a,i,datename,number,index,midd,midd_1,midd_2)
write.csv(train,"E:/R 3.4.2 for Windows/O2O_tc/train_two.csv")
########################################### revised  #####################################
########### 整个期间与Coupon_id相关###################
#---特殊号码的优惠卷被领取的总次数 Coupon_get_count---
midd <- revised_offline%>%select(Coupon_id,Date_received)%>%filter(Coupon_id != 'null')%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_count = n())
revised <- merge(revised,midd,by = 'Coupon_id',all.x = T)
revised$Coupon_get_count[is.na(revised$Coupon_get_count) == T] <- 0
#----客户领取特殊号码的优惠卷未购买的次数 Coupon_get_no_buy_count----
midd <- revised_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_no_buy_count = n())
revised <- merge(revised,midd,by = 'Coupon_id',all.x = T)
revised$Coupon_get_no_buy_count[is.na(revised$Coupon_get_no_buy_count) == T] <- 0
#----客户领取特殊号码的优惠卷并且已经购买的次数 Coupon_get_buy_count-----------------
midd <- revised_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_buy_count = n())
revised <- merge(revised,midd,by = 'Coupon_id',all.x = T)
revised$Coupon_get_buy_count[is.na(revised$Coupon_get_buy_count) == T] <- 0
#---客户领取特殊号码优惠卷并且在15日内购买的次数 Coupon_get_buy_15----------
midd <- revised_offline%>%select(Coupon_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Coupon_id)%>%summarise(Coupon_get_buy_15 = n())
revised <- merge(revised,midd,by = 'Coupon_id',all.x = T)
revised$Coupon_get_buy_15[is.na(revised$Coupon_get_buy_15) == T] <- 0
#---客户领取特殊号码的优惠卷并购买的次数  但不在15日内（线下）Coupon_get_buy_not15-----
revised$Coupon_get_buy_not15 <- revised$Coupon_get_buy_count - revised$Coupon_get_buy_15
#----特殊号码优惠卷的负样本占比（领卷已购买但不在15天之内及领了就没买）Coupon_get_negative_rate --
revised$Coupon_get_negative_rate <- round( (revised$Coupon_get_buy_not15 + revised$Coupon_get_no_buy_count) / 
                                             revised$Coupon_get_count,digits = 3)
revised$Coupon_get_negative_rate[is.na(revised$Coupon_get_negative_rate) == T] <- 0.000
#-----特殊号码优惠卷 已领卷但在15日后购买 占比 Coupon_get_negative_15rate ------
revised$Coupon_get_negative_15rate <- round( revised$Coupon_get_buy_not15 / revised$Coupon_get_count,digits = 3)
revised$Coupon_get_negative_15rate[is.na(revised$Coupon_get_negative_15rate) == T] <- 0.000
#-----特殊号码优惠卷 正样本 （领取并在15日内购买）占比 Coupon_positive_rate------------------
revised$Coupon_positive_rate<- round(revised$Coupon_get_buy_15 / revised$Coupon_get_count,digits = 3)
revised$Coupon_positive_rate[is.na(revised$Coupon_positive_rate) == T] <- 0.000
########### 整个期间与领取购买相关历史记录 ################
#---客户领取优惠卷的总次数（线下）get_count_off----    
midd <- revised_offline%>%select(User_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id)%>%summarise(get_count_off=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_count_off[is.na(revised$get_count_off) == T] <- 0
#----客户领取优惠卷未购买的次数（线下）get_no_buy_off----
midd <- revised_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id)%>%summarise(get_no_buy_off=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_no_buy_off[is.na(revised$get_no_buy_off) == T] <- 0
#----客户领取优惠卷并且已经购买的次数（线下）get_buy_off-----------------
midd <- revised_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id)%>%summarise(get_buy_off=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_buy_off[is.na(revised$get_buy_off) == T] <- 0
#-------------客户什么卷也不用的普通购买次数（线下）nodiscount_buy_off--------
midd <- revised_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id)%>%summarise(nodiscount_buy_off = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$nodiscount_buy_off[is.na(revised$nodiscount_buy_off) == T] <- 0
#------客户领取优惠卷并且在15日内购买的次数（线下）get_buy_off15----------
midd <- revised_offline%>%select(User_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id)%>%summarise(get_buy_off15=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_buy_off15[is.na(revised$get_buy_off15) == T] <- 0
#------客户领取优惠卷并购买的次数  但不在15日内（线下）get_buy_off_not15----------
revised$get_buy_off_not15 <- revised$get_buy_off - revised$get_buy_off15
#------------客户领取满减卷的次数（线下）get_mj_off---------------
midd <- revised_offline%>%select(User_id,Discount_fac)%>%filter(Discount_fac == 1)%>%
  group_by(User_id)%>%summarise(get_mj_off = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_mj_off[is.na(revised$get_mj_off) == T] <- 0
#-----------客户领取折扣卷的次数（线下） get_zk_off---------------------
midd <- revised_offline%>%select(User_id,Discount_fac)%>%filter(Discount_fac == 0)%>%
  group_by(User_id)%>%summarise(get_zk_off = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_zk_off[is.na(revised$get_zk_off) == T] <- 0
#----------------客户使用满减卷购买的总次数（线下） mj_buy_off-------------------------
midd <- revised_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 1 )%>%group_by(User_id)%>%summarise( mj_buy_off = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$mj_buy_off[is.na(revised$mj_buy_off) == T] <- 0
#----------------客户使用满减卷并在15日内购买的次数（线下） mj_buy_off15-------------------------
midd <- revised_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 1 )%>%group_by(User_id)%>%summarise( mj_buy_off15 = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$mj_buy_off15[is.na(revised$mj_buy_off15) == T] <- 0
#----------------客户使用折扣卷购买的总次数（线下） zk_buy_off---------------
midd <- revised_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 0 )%>%group_by(User_id)%>%summarise( zk_buy_off = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$zk_buy_off[is.na(revised$zk_buy_off) == T] <- 0
#----------------客户使用折扣卷并在15日内购买的总次数（线下） zk_buy_off15---------------
midd <- revised_offline%>%select(User_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 0 )%>%group_by(User_id)%>%summarise( zk_buy_off15 = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$zk_buy_off15[is.na(revised$zk_buy_off15) == T] <- 0
#-----------客户 满减卷正样本 占比 mj_positive_rate------------------
revised$mj_positive_rate <- round(revised$mj_buy_off15 / revised$get_mj_off,digits = 3)
revised$mj_positive_rate[is.na(revised$mj_positive_rate) == T] <- 0.000
#-----------客户 折扣卷正样本 占比 zk_positive_rate------------------
revised$zk_positive_rate <- round(revised$zk_buy_off15 / revised$get_zk_off,digits = 3)
revised$zk_positive_rate[is.na(revised$zk_positive_rate) == T] <- 0.000
#-----------客户 无卷购买占比 nodiscount_rate-------------------
revised$nodiscount_rate <- round(revised$nodiscount_buy_off / 
                                   (revised$get_buy_off + revised$nodiscount_buy_off),digits = 3)
revised$nodiscount_rate[is.na(revised$nodiscount_rate) == T] <- 0.000
#-----------客户 用卷正样本 占比 get_positive_rate-------------------
revised$get_positive_rate <- round(revised$get_buy_off15 / revised$get_count_off,digits = 3)
revised$get_positive_rate[is.na(revised$get_positive_rate) == T] <- 0.000
#-----------客户 用卷负样本样本（领卷已购买但不在15天之内及领了就没买） 占比 get_negative_rate ------
revised$get_negative_rate <- round( (revised$get_buy_off_not15 + revised$get_no_buy_off) / 
                                      revised$get_count_off,digits = 3)
revised$get_negative_rate[is.na(revised$get_negative_rate) == T] <- 0.000
#-----------客户 用卷15日后购买 占比 get_negative_15rate ------
revised$get_negative_15rate <- round( revised$get_buy_off_not15 / revised$get_count_off,digits = 3)

revised$get_negative_15rate[is.na(revised$get_negative_15rate) == T] <- 0.000
#----------客户 正样本 与 总购买次数的占比 buy_positive_rate------------------
revised$buy_positive_rate <- round(revised$get_buy_off15 /
                                     (revised$nodiscount_buy_off + revised$get_buy_off),digits = 3)
revised$buy_positive_rate[is.na(revised$buy_positive_rate) == T] <- 0.000
gc()
################### 与日期相关的购买 ############################
#-----------------------客户平均购买时间间隔（所有购买）---------------------
midd <- revised_offline%>%select(User_id,Date)%>%group_by(User_id)%>%summarise( min_Date = min(Date,na.rm = T),
                                                                                max_Date = max(Date,na.rm = T))
midd$avg_datediff_off <- midd$max_Date-midd$min_Date
midd <- midd[-c(2,3)]
revised <- merge(revised,midd,by = 'User_id',all.x = T)
midd <- revised_offline%>%select(User_id,buy_fac)%>%filter(buy_fac != 2)%>%
  group_by(User_id)%>%summarise(cs = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$avg_datediff_off <- round(revised$avg_datediff_off/revised$cs,digits = 2)
revised <- subset(revised, select = -c(cs) ) #用变量名取子集
revised$avg_datediff_off[is.na(revised$avg_datediff_off) == T] <- 0
#------------------客户领取优惠卷的当日是本周的第几天 isweekday----------
revised$weekday_r <- as.factor(revised$weekday_r)
revised$weekday_r <- ordered(revised$weekday_r,levels = c('星期一','星期二','星期三','星期四',
                                                          '星期五','星期六','星期日'))
revised$is_weekday <- unclass(revised$weekday_r)
#------------------客户领取优惠卷的当日是否周末 is_weekend------------
revised$is_weekend <- 0
revised$is_weekend <- ifelse(revised$is_weekday == 6 | revised$is_weekday == 7,1,0)
revised$is_weekend <- as.numeric(revised$is_weekend)
#-------------客户周一领卷并购买的次数（线下） received_mon 及 mon_rate率------------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期一' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_mon = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_mon[is.na(revised$received_mon) == T] <- 0
revised$mon_rate <- round(revised$received_mon / revised$get_buy_off,digits = 3)
revised$mon_rate[is.na(revised$mon_rate) == T] <- 0.000
#---------------0客户周二领并购买卷的次数（线下）  received_tur 及 tur_rate率-----------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期二' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_tur = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_tur[is.na(revised$received_tur) == T] <- 0
revised$tur_rate <- round(revised$received_tur / revised$get_buy_off,digits = 3)
revised$tur_rate[is.na(revised$tur_rate) == T] <- 0.000
#-----------客户周三领卷并购买的次数（线下）  received_wen 及 wen_rate率-------------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期三' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_wen = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_wen[is.na(revised$received_wen) == T] <- 0
revised$wen_rate <- round(revised$received_wen / revised$get_buy_off,digits = 3)
revised$wen_rate[is.na(revised$wen_rate) == T] <- 0.000
#---------------客户周四领卷并购买的次数（线下）   received_thu及 thu_rate率--------------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期四' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_thu = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_thu[is.na(revised$received_thu) == T] <- 0
revised$thu_rate <- round(revised$received_thu / revised$get_buy_off,digits = 3)
revised$thu_rate[is.na(revised$thu_rate) == T] <- 0.000
#---------客户周五领卷并购买的次数（线下）   received_fri 及 fri_rate率-------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期五' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_fri = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_fri[is.na(revised$received_fri) == T] <- 0
revised$fri_rate <- round(revised$received_fri / revised$get_buy_off,digits = 3)
revised$fri_rate[is.na(revised$fri_rate) == T] <- 0.000
#---------客户周六领卷并购买的次数（线下）   received_sat 及 sat_rate率-------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期六' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_sat = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_sat[is.na(revised$received_sat) == T] <- 0
revised$sat_rate <- round(revised$received_sat / revised$get_buy_off,digits = 3)
revised$sat_rate[is.na(revised$sat_rate) == T] <- 0.000
#---------客户周日领卷并购买的次数（线下）   received_sun 及 sun_rate率-------------
midd <- revised_offline%>%select(User_id,weekday_r,buy_fac)%>%
  filter(weekday_r == '星期日' & buy_fac == 1)%>%
  group_by(User_id)%>%summarise( received_sun = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$received_sun[is.na(revised$received_sun) == T] <- 0
revised$sun_rate <- round(revised$received_sun / revised$get_buy_off,digits = 3)
revised$sun_rate[is.na(revised$sun_rate) == T] <- 0.000
#-----------客户工作日领取优惠卷的次数--received_workday -------------
revised$received_workday <- revised$received_mon + revised$received_tur + revised$received_wen + 
  revised$received_thu + revised$received_fri
#--------------客户周末领取优惠卷的次数	received_weekend 及 weekenf_rate率- --------
revised$received_weekend <- revised$received_sat + revised$received_sun
revised$weekenf_rate <- round(revised$received_weekend / revised$get_buy_off,digits = 3)
revised$weekenf_rate[is.na(revised$weekenf_rate) == T] <- 0.000
################### 与距离相关的 ############################
#-------线下购买最长最短，平均距离  ----------------
attr(revised_offline$Distance,'levels')
revised_offline$Distance <- ordered(revised_offline$Distance,
                                    levels = c('0','1','2','3','4','5','6','7','8','9','10','null'))
revised_offline$Distance <- as.character(revised_offline$Distance)
revised_offline$Distance[revised_offline$Distance == 'null'] <- NA
revised_offline$Distance <- as.numeric(revised_offline$Distance)   #直接转会因排序问题引起数据乱，故先排序再字符再数值
midd <- revised_offline%>%select(User_id,Distance)%>%group_by(User_id)%>%
  summarise(max_distance = max(Distance,na.rm = T),min_distance = min(Distance,na.rm = T),
            avg_Dis = mean(Distance,na.rm = T))
midd$max_distance[midd$max_distance == -Inf] <- NA
midd$min_distance[midd$min_distance == Inf] <- NA
midd$avg_Dis <- round(midd$avg_Dis,digits = 2)
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$max_distance[is.na(revised$max_distance) == T] <- 1
revised$min_distance[is.na(revised$min_distance) == T] <- 0
revised$avg_Dis[is.na(revised$avg_Dis) == T] <- 0.8
#--- 不同距离供应商所发的卷  被领取的总次数 ---- 
midd <- revised_offline%>%select(Distance_copy,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Distance_copy)%>%summarise(Distance_get_count = n())
revised <- merge(revised,midd,by = 'Distance_copy',all.x = T)
#----不同距离供应商所发的卷  领取未购买的次数 Distance_get_no_buy --------
midd <- revised_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_no_buy = n())
revised <- merge(revised,midd,by = 'Distance_copy',all.x = T)
#----不同距离供应商所发的卷  被购买的总次数 Distance_get_buy-----------------
midd <- revised_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_buy = n())
revised <- merge(revised,midd,by = 'Distance_copy',all.x = T)
#-----不同距离供应商所发的卷  在15日内购买的次数 Distance_get_buy15----------
midd <- revised_offline%>%select(Distance_copy,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Distance_copy)%>%summarise(Distance_get_buy15 = n())
revised <- merge(revised,midd,by = 'Distance_copy',all.x = T)
#------不同距离供应商所发的卷  在15日内购买率 Distance_get_buy15_rate----------
revised$Distance_get_buy15_rate <- round(revised$Distance_get_buy15 / revised$Distance_get_count,digits = 3)
gc()
######################## 与供应商相关 ###############################
#--- 供应商被领取优惠卷的总次数 ----    
midd <- revised_offline%>%select(Merchant_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Merchant_id)%>%summarise(M_get_count_off=n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_count_off[is.na(revised$M_get_count_off) == T] <- 0
#----供应商被领取优惠卷未购买的次数  M_get_no_buy_off--------
midd <- revised_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Merchant_id)%>%summarise(M_get_no_buy_off=n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_no_buy_off[is.na(revised$M_get_no_buy_off) == T] <- 0
#----供应商被领取优惠卷并且已经购买的次数 M_get_buy_off-----------------
midd <- revised_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Merchant_id)%>%summarise(M_get_buy_off=n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_buy_off[is.na(revised$M_get_buy_off) == T] <- 0
#-------------供应商什么卷也不用的普通购买次 M_nodiscount_buy_off--------
midd <- revised_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(Merchant_id)%>%summarise(M_nodiscount_buy_off = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_nodiscount_buy_off[is.na(revised$M_nodiscount_buy_off) == T] <- 0
#------供应商领取优惠卷并且在15日内购买的次数 M_get_buy_off15----------
midd <- revised_offline%>%select(Merchant_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Merchant_id)%>%summarise(M_get_buy_off15=n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_buy_off15[is.na(revised$M_get_buy_off15) == T] <- 0
#------供应商领取优惠卷并购买的次数  没在15日内  M_get_buy_off_not15----------
revised$M_get_buy_off_not15 <- revised$M_get_buy_off - revised$M_get_buy_off15
#------------供应商领取满减卷的次数  M_get_mj_off---------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac)%>%filter(Discount_fac == 1)%>%
  group_by(Merchant_id)%>%summarise(M_get_mj_off = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_mj_off[is.na(revised$M_get_mj_off) == T] <- 0
#-----------供应商领取折扣卷的次数  M_get_zk_off---------------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac)%>%filter(Discount_fac == 0)%>%
  group_by(Merchant_id)%>%summarise(M_get_zk_off = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_get_zk_off[is.na(revised$M_get_zk_off) == T] <- 0
#----------------供应商使用满减卷购买的总次数  M_mj_buy_off-------------------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 1 )%>%group_by(Merchant_id)%>%summarise( M_mj_buy_off = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_mj_buy_off[is.na(revised$M_mj_buy_off) == T] <- 0
#----------------供应商使用满减卷并在15日内购买的次数   M_mj_buy_off15-------------------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 1 )%>%group_by(Merchant_id)%>%summarise( M_mj_buy_off15 = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_mj_buy_off15[is.na(revised$M_mj_buy_off15) == T] <- 0
#----------------供应商使用折扣卷购买的总次数 M_zk_buy_off---------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0 )%>%
  filter(Discount_fac == 0 )%>%group_by(Merchant_id)%>%summarise( M_zk_buy_off = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_zk_buy_off[is.na(revised$M_zk_buy_off) == T] <- 0
#----------------供应商使用折扣卷并在15日内购买的总次数  M_zk_buy_off15---------------
midd <- revised_offline%>%select(Merchant_id,Discount_fac,buy_fac)%>%filter(buy_fac == 1)%>%
  filter(Discount_fac == 0 )%>%group_by(Merchant_id)%>%summarise( M_zk_buy_off15 = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$M_zk_buy_off15[is.na(revised$M_zk_buy_off15) == T] <- 0
#-----------供应商 满减卷正样本 占比 M_mj_positive_rate------------------
revised$M_mj_positive_rate <- round(revised$M_mj_buy_off15 / revised$M_get_mj_off,digits = 3)
revised$M_mj_positive_rate[is.na(revised$M_mj_positive_rate) == T] <- 0.000
#-----------供应商 折扣卷正样本 占比 M_zk_positive_rate------------------
revised$M_zk_positive_rate <- round(revised$M_zk_buy_off15 / revised$M_get_zk_off,digits = 3)
revised$M_zk_positive_rate[is.na(revised$M_zk_positive_rate) == T] <- 0.000
#-----------供应商 无卷购买占比 nodiscount_rate-------------------
revised$M_nodiscount_rate <- round(revised$M_nodiscount_buy_off / 
                                     (revised$M_get_buy_off + revised$M_nodiscount_buy_off),digits = 3)
revised$M_nodiscount_rate[is.na(revised$M_nodiscount_rate) == T] <- 0.000
#-----------供应商 用卷正样本 占比 get_positive_rate-------------------
revised$M_get_positive_rate <- round(revised$M_get_buy_off15 / revised$M_get_count_off,digits = 3)
revised$M_get_positive_rate[is.na(revised$M_get_positive_rate) == T] <- 0.000
#-----------供应商 用卷负样本样本（领卷已购买但不在15天之内及领了就没买） 占比 get_negative_rate ------
revised$M_get_negative_rate <- round( (revised$M_get_buy_off_not15 + revised$M_get_no_buy_off) / 
                                        revised$M_get_count_off,digits = 3)
revised$M_get_negative_rate[is.na(revised$M_get_negative_rate) == T] <- 0.000
#-----------供应商 用卷15日后购买 占比 get_negative_15rate ------
revised$M_get_negative_15rate <- round( revised$M_get_buy_off_not15 / revised$M_get_count_off,digits = 3)
revised$M_get_negative_15rate[is.na(revised$M_get_negative_15rate) == T] <- 0.000
#----------供应商 正样本 与 总购买次数的占比 buy_positive_rate------------------
revised$M_buy_positive_rate <- round(revised$M_get_buy_off15 /
                                       (revised$M_nodiscount_buy_off + revised$M_get_buy_off),digits = 3)
revised$M_buy_positive_rate[is.na(revised$M_buy_positive_rate) == T] <- 0.000
gc()
##################### 与折扣相关 ############################
#--- 每种折扣被领取的总次数 ----    
midd <- revised_offline%>%select(Discount_rate,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(Discount_rate)%>%summarise(rate_get_count = n())
revised <- merge(revised,midd,by = 'Discount_rate',all.x = T)
revised$rate_get_count[is.na(revised$rate_get_count) == T] <- 0
#----每种折扣领取未购买的次数 rate_get_no_buy --------
midd <- revised_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(Discount_rate)%>%summarise(rate_get_no_buy = n())
revised <- merge(revised,midd,by = 'Discount_rate',all.x = T)
revised$rate_get_no_buy[is.na(revised$rate_get_no_buy) == T] <- 0
#----每种折扣被购买的总次数 rate_get_buy-----------------
midd <- revised_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(Discount_rate)%>%summarise(rate_get_buy = n())
revised <- merge(revised,midd,by = 'Discount_rate',all.x = T)
revised$rate_get_buy[is.na(revised$rate_get_buy) == T] <- 0
#------每种折扣在15日内购买的次数 rate_get_buy15----------
midd <- revised_offline%>%select(Discount_rate,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(Discount_rate)%>%summarise(rate_get_buy15 = n())
revised <- merge(revised,midd,by = 'Discount_rate',all.x = T)
revised$rate_get_buy15[is.na(revised$rate_get_buy15) == T] <- 0
#------每种折扣在15日内购买率 rate_get_buy15_rate----------
revised$rate_get_buy15_rate <- round(revised$rate_get_buy15 / revised$rate_get_count,digits = 3)
revised$rate_get_buy15_rate[is.na(revised$rate_get_buy15_rate) == T] <- 0.000
############ 与预测集 User_id与Merchant,Discount 相关 判断类变量 ######################
#------客户所领取的卷 是不是 TOP10的优惠卷  is_top10 ----------
midd <- sort(unique(revised$rate_get_buy15_rate),decreasing = T)
midd <- midd[1:10]
revised$is_top10 <- ifelse(revised$rate_get_buy15_rate %in% midd,1,0)
##------客户所领取的卷 是不是 首次发行的  is_new_discount_rate ----------
revised$is_new_discount_rate <- ifelse(revised$rate_get_count == 0,1,0)
#-------------预测集中领取优惠卷的客户是否在已购供应商范围之内   M_is_in------------------------
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac != 2)%>%group_by(User_id)
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Merchant_id)%>%group_by(User_id)%>%
  summarise(items = paste(Merchant_id, collapse=',') )   #先去重重值再生成已购（含普通购买）供应商向量
revised <- merge(revised,midd,by = 'User_id',all.x = T)
#这里贴过来的items中有NA，因为有的客户以前集中就没见好就购买过，或是本月新客户
grepFun <- function(revised){
  grepl(revised['Merchant_id'],revised['items'],fixed=TRUE)
}                                                #grepl不能向量化，只能定义个函数再APPLY
revised$M_is_in <- apply(revised,1,grepFun)
revised$M_is_in[revised$M_is_in == TRUE] <- 1
revised$M_is_in[revised$M_is_in == FALSE] <- 0
revised <- subset(revised,select = -c(items))       #删除列 items
#----------预测集中领取优惠卷的客户是否在已用优惠卷购买过的供应商范围之内(不含普通购买和超15日购买) M_is_in_getbuy---
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Merchant_id)%>%group_by(User_id)%>%
  summarise(items = paste(Merchant_id, collapse=',') )   #先去重重值再生成已购（含普通购买）供应商向量
revised <- merge(revised,midd,by = 'User_id',all.x = T)
#这里贴过来的items中有NA，因为有的客户以前集中就没见好就购买过，或是本月新客户
grepFun <- function(revised){
  grepl(revised['Merchant_id'],revised['items'],fixed=TRUE)
}                                                #grepl不能向量化，只能定义个函数再APPLY
revised$M_is_in_getbuy <- apply(revised,1,grepFun)
revised$M_is_in_getbuy[revised$M_is_in_getbuy == TRUE] <- 1
revised$M_is_in_getbuy[revised$M_is_in_getbuy == FALSE] <- 0
revised <- subset(revised,select = -c(items))       #删除列 items
#---------客户所领的卷的折扣 ，是否之前也在线下购买过  Discount_is_buy_off--------------------
midd <- revised_offline%>%select(User_id,Discount_rate,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)  
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Discount_rate)%>%group_by(User_id)%>%
  summarise(items = paste(Discount_rate, collapse=',') )   
revised <- merge(revised,midd,by = 'User_id',all.x = T)
grepFun <- function(revised){
  grepl(revised['Discount_rate'],revised['items'],fixed=TRUE)
}                                                
revised$Discount_is_buy_off <- apply(revised,1,grepFun)
revised$Discount_is_buy_off[revised$Discount_is_buy_off == TRUE] <- 1
revised$Discount_is_buy_off[revised$Discount_is_buy_off == FALSE] <- 0
revised <- subset(revised,select = -c(items))      
#-------------------------此客户是否是第一次领取优惠卷 is_first------------------
midd_1 <- revised_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)
midd_2 <- revised%>%select(User_id,Date_received)
midd <- rbind(midd_1,midd_2)
midd <- midd%>%select(User_id,Date_received)%>%group_by(User_id)%>%
  summarise(min = min(Date_received,na.rm = T))
revised <- merge(revised,midd,by = 'User_id',all.x = T)  
revised$is_first <- 0
revised$is_first[(revised$min >= revised$Date_received) == T] <- 1
revised <- subset(revised,select = -c(min))
#-------------客户之前是否也领取过该号码的特殊优惠卷 is_get_Coupon-----------
midd <- revised_offline%>%select(User_id,Coupon_id,Date_received)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Coupon_id, .keep_all = TRUE)   #distinct的多列去重
midd <- midd%>%select(User_id,Coupon_id)%>%group_by(User_id)%>%
  summarise(items = paste(Coupon_id, collapse=',') )
revised <- merge(revised,midd,by = 'User_id',all.x = T)
grepFun <- function(revised){
  grepl(revised['Coupon_id'],revised['items'],fixed=TRUE)
}                                                
revised$is_get_Coupon <- apply(revised,1,grepFun)
revised$is_get_Coupon[revised$is_get_Coupon == TRUE] <- 1
revised$is_get_Coupon[revised$is_get_Coupon == FALSE] <- 0
revised <- subset(revised,select = -c(items))   

#------------客户之前是否购买过该号码的特殊优惠卷  is_buy_Coupon-----------
midd <- revised_offline%>%select(User_id,Coupon_id,buy_fac)%>%
  filter(buy_fac == 1 |buy_fac == 0 )%>%group_by(User_id)  
midd <- midd%>%select(User_id,Coupon_id)%>%group_by(User_id)%>%
  summarise(items = paste(Coupon_id, collapse=',') )
revised <- merge(revised,midd,by = 'User_id',all.x = T)
grepFun <- function(revised){
  grepl(revised['Coupon_id'],revised['items'],fixed=TRUE)
}                                                
revised$is_buy_Coupon <- apply(revised,1,grepFun)
revised$is_buy_Coupon[revised$is_buy_Coupon == TRUE] <- 1
revised$is_buy_Coupon[revised$is_buy_Coupon == FALSE] <- 0
revised <- subset(revised,select = -c(items))   
######################### 预测集中 客户与特殊号码卷的交互变量 ##################################
#----------预测集中的客户 领取该号码优惠卷 的总次数 U_C_getcount---
midd <- revised_offline%>%select(User_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_getcount = n())  
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T)  
revised$U_C_getcount[is.na(revised$U_C_getcount) == T] <- 0
#----预测集中的客户 领取该号码优惠卷 并且已购买的次数 U_C_buycount -----------
midd <- revised_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_buycount = n())  
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T) 
revised$U_C_buycount[is.na(revised$U_C_buycount) == T] <- 0
#----预测集中的客户 领取该号码优惠卷 并且在15日内购买的次数 U_C_buy_count15---
midd <- revised_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_buy_count15 = n())  
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T) 
revised$U_C_buy_count15[is.na(revised$U_C_buy_count15) == T] <- 0
#---预测集中的客户 领取该号码优惠卷 但并未购买的次数 U_C_get_no_buy_count-----
midd <- revised_offline%>%select(User_id,Coupon_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Coupon_id)%>%summarise(U_C_get_no_buy_count = n())  
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T) 
revised$U_C_get_no_buy_count[is.na(revised$U_C_get_no_buy_count) == T] <- 0
#----预测集中的客户 在该号码上的 正样本率 U_C_getbuy_rate15-------
revised$U_C_getbuy_rate15 <- round(revised$U_C_buy_count15 / revised$U_C_getcount,digits = 3)
revised$U_C_getbuy_rate15[is.na(revised$U_C_getbuy_rate15) == T] <- 0
#------预测集中的客户历史上领取过多少个不同的特殊号码U_Couponid_count------
midd <- revised_offline%>%select(User_id,Coupon_id,Date_received)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Coupon_id, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Couponid_count = n())
revised <- merge(revised,midd_1,by = 'User_id',all.x = T) 
revised$U_Couponid_count[is.na(revised$U_Couponid_count) == T] <- 0
#-----预测集中的客户在该号码上正样本占比 U_C_positive_rate-----
#该号码的正样本数量/历史上总的正样本数量
revised$U_C_positive_rate <-  round(revised$U_C_buy_count15 / revised$Coupon_get_buy_15,digits = 3)
revised$U_C_positive_rate[is.na(revised$U_C_positive_rate) == T] <- 0
#-----预测集中的客户在该号码上负样本占比 U_C_nagtive_rate----
#该号码的负样本数量/历史上总的负样本数量
revised$U_C_nagtive_rate <-  round(revised$U_C_get_no_buy_count / revised$Coupon_get_no_buy_count,digits = 3)
revised$U_C_nagtive_rate[is.na(revised$U_C_nagtive_rate) == T] <- 0
######################### 预测集中 客户与供应商交互变量 ##################################
#---预测集中的客户在该供应商 领取的总次数 U_M_getcount-----------------
midd <- revised_offline%>%select(User_id,Merchant_id,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_getcount = n())  
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T)  
revised$U_M_getcount[is.na(revised$U_M_getcount) == T] <- 0
#---预测集中的客户在该供应商 领取并购买的总次数 U_M_get_buy_count-----------------
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_get_buy_count = n())  
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T) 
revised$U_M_get_buy_count[is.na(revised$U_M_get_buy_count) == T] <- 0
#---预测集中的客户在该供应商 无卷购买的总次数 U_M_nodiscount-----------------
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_nodiscount = n())   
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
revised$U_M_nodiscount[is.na(revised$U_M_nodiscount) == T] <- 0
#---预测集中的客户在该供应商 领卷无购买的总次数 U_M_get_no_buy_count-----------------
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_get_no_buy_count = n())   
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
revised$U_M_get_no_buy_count[is.na(revised$U_M_get_no_buy_count) == T] <- 0
#---预测集中的客户在该供应商 领取并在15日内购买次数 U_M_buycount15-----------------
midd <- revised_offline%>%select(User_id,Merchant_id,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Merchant_id)%>%summarise(U_M_buycount15 = n())   #同一客户同一供应商，购买次数
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T)  #两个关健字的MERGE
revised$U_M_buycount15[is.na(revised$U_M_buycount15) == T] <- 0
#---预测集中的客户在该供应商 正样本率（领取并在15日内购买占比） U_M_get_buy_rate15-----------------
revised$U_M_get_buy_rate15 <- round(revised$U_M_buycount15 / revised$U_M_getcount,digits = 3)
revised$U_M_get_buy_rate15[is.na(revised$U_M_get_buy_rate15) == T] <- 0
#---预测集中的客户历史上领取过优惠卷的有多少个不同的供应商 U_Merchant_id_count----
midd <- revised_offline%>%select(User_id,Merchant_id,Coupon_id)%>%
  filter(Coupon_id != "null")%>%group_by(User_id)  
midd <- midd %>% distinct(User_id,Merchant_id, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Merchant_id_count = n())
revised <- merge(revised,midd_1,by = 'User_id',all.x = T) 
revised$U_Merchant_id_count[is.na(revised$U_Merchant_id_count) == T] <- 0
#------------预测集中的客户在该供应商正样本占比 U_M_positive_rate-------------
#该供应的正样本数量/历史上的供应商中总的正样本数量
revised$U_M_positive_rate = round(revised$U_M_get_buy_rate15 / revised$get_buy_off15,digits = 3)
revised$U_M_positive_rate[is.na(revised$U_M_positive_rate) == T] <- 0
#------------预测集中的客户在该供应商负样本占比  U_M_nagtive_rate-------------
#该供应的负样本数量/历史上的供应商中总的负样本数量）
revised$U_M_nagtive_rate = round(revised$U_M_get_no_buy_count / revised$get_no_buy_off,digits = 3)
revised$U_M_nagtive_rate[is.na(revised$U_M_nagtive_rate) == T] <- 0
#-----对比供应商平均正样本率的变化量 U_M_avg_pos_incremental ---------
#预测集中的客户在该供应商正样本占比 - 正样本率供应商平均值
revised$U_M_avg_pos_incremental <- revised$U_M_positive_rate - 
  revised$get_buy_off15/revised$get_count_off/revised$U_Merchant_id_count
revised$U_M_avg_pos_incremental <- round(revised$U_M_avg_pos_incremental,digits = 3)
revised$U_M_avg_pos_incremental[is.na(revised$U_M_avg_pos_incremental) == T] <- 0
#-----对比供应商平均负样本率的变化量 U_M_avg_nag_incremental ---------
#预测集中的客户在该供应商负样本占比 - 负样本率供应商平均值
revised$U_M_avg_nag_incremental <- revised$U_M_nagtive_rate - 
  (revised$get_buy_off_not15 + revised$get_no_buy_off)/revised$get_count_off/revised$U_Merchant_id_count
revised$U_M_avg_nag_incremental <- round(revised$U_M_avg_nag_incremental,digits = 3)
revised$U_M_avg_nag_incremental[is.na(revised$U_M_avg_nag_incremental) == T] <- 0
######################### 预测集中 客户与折扣率交互变量 ##################################
#---预测集中的客户所领卷的折扣率  历史领取的总次数 U_D_getcount-----------------
midd <- revised_offline%>%select(User_id,Discount_rate,Coupon_id)%>%filter(Coupon_id != 'null')%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_getcount = n())  
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T)  
revised$U_D_getcount[is.na(revised$U_D_getcount) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领取并购买的总次数 U_D_get_buy_count-----------------
midd <- revised_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 0)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_get_buy_count = n())  
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T) 
revised$U_D_get_buy_count[is.na(revised$U_D_get_buy_count) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领卷无购买的总次数 U_D_get_no_buy_count-----------------
midd <- revised_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_get_no_buy_count = n())   
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T)  #两个关健字的MERGE
revised$U_D_get_no_buy_count[is.na(revised$U_D_get_no_buy_count) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史领取并在15日内购买次数 U_D_buycount15-----------------
midd <- revised_offline%>%select(User_id,Discount_rate,buy_fac)%>%filter(buy_fac == 1)%>%
  group_by(User_id,Discount_rate)%>%summarise(U_D_buycount15 = n())   #同一客户同一供应商，购买次数
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T)  #两个关健字的MERGE
revised$U_D_buycount15[is.na(revised$U_D_buycount15) == T] <- 0
#---预测集中的客户所领卷的折扣率  历史 正样本率（领取并在15日内购买占比） U_D_get_buy_rate15-----------------
revised$U_D_get_buy_rate15 <- round(revised$U_D_buycount15 / revised$U_D_getcount,digits = 3)
revised$U_D_get_buy_rate15[is.na(revised$U_D_get_buy_rate15) == T] <- 0
#-----------预测集中的客户历史上领取过多少个不同折扣U_Discountid_count-----
midd <- revised_offline%>%select(User_id,Discount_rate)%>%
  filter(Discount_rate != "null")%>%group_by(User_id)  
midd <- distinct(midd, .keep_all = TRUE)   #distinct的多列去重
midd_1 <- midd%>%group_by(User_id)%>%summarise(U_Discountid_count = n())
revised <- merge(revised,midd_1,by = 'User_id',all.x = T) 
revised$U_Discountid_count[is.na(revised$U_Discountid_count) == T] <- 0
#------预测集中的客户在该折扣上正样本占比--U_D_positive_rate--------------
#（ 该折扣上的正样本数量/历史上总的正样本数量）
revised$U_D_positive_rate <- round(revised$U_D_buycount15 / revised$get_buy_off15,digits = 3)
revised$U_D_positive_rate[is.na(revised$U_D_positive_rate) == T] <- 0
#--------预测集中的客户在该折扣上负样本占比--U_D_nagtive_rate------------
#（ 该折扣上的负样本数量/历史上总的负样本数量）
revised$U_D_nagtive_rate = round(revised$U_D_get_no_buy_count / revised$get_no_buy_off,digits = 3)
revised$U_D_nagtive_rate[is.na(revised$U_D_nagtive_rate) == T] <- 0
#-----对比 折扣率 平均正样本率的变化量 U_D_avg_pos_incremental ---------
#预测集中的客户在该折扣率正样本占比 - 正样本率折扣率平均值
revised$U_D_avg_pos_incremental <- revised$U_D_positive_rate - 
  revised$get_buy_off15/revised$get_count_off/revised$U_Discountid_count
revised$U_D_avg_pos_incremental <- round(revised$U_D_avg_pos_incremental,digits = 3)
revised$U_D_avg_pos_incremental[is.na(revised$U_D_avg_pos_incremental) == T] <- 0
#-----对比 折扣率 平均负样本率的变化量 U_D_avg_nag_incremental ---------
#预测集中的客户在该折扣率负样本占比 - 负样本率折扣平均值
revised$U_D_avg_nag_incremental <- revised$U_D_nagtive_rate - 
  (revised$get_buy_off_not15 + revised$get_no_buy_off)/revised$get_count_off/
  revised$U_Discountid_count
revised$U_D_avg_nag_incremental <- round(revised$U_D_avg_nag_incremental,digits = 3)
revised$U_D_avg_nag_incremental[is.na(revised$U_D_avg_nag_incremental) == T] <- 0
gc()
######################## 重编码 ######################
#----------------供应商重编码     Merchant_woe---------------------------------------
revised_M <- data.frame(revised$Merchant_id)  
revised_M <- unique(revised_M)
revised_M <- merge(revised_M,Merchant_woe,by.x = 'revised.Merchant_id',by.y = 'kind',all.x = T)
revised_M$Merchant_woe[is.na(revised_M$Merchant_woe) == T] <- 1.02379870536
revised <- merge(revised,revised_M,by.x = 'Merchant_id',by.y = 'revised.Merchant_id',all.x = T)
#-----------------------折扣率重编码   Discount_woe-------------------------------
revised_D <- data.frame(revised$Discount_rate)  
revised_D <- unique(revised_D)
revised_D <- merge(revised_D,Discount_woe,by.x = 'revised.Discount_rate',by.y = 'kind',all.x = T)
revised_D$Discount_woe[is.na(revised_D$Discount_woe) == T] <- 0.00050207103
revised <- merge(revised,revised_D,by.x = 'Discount_rate',by.y = 'revised.Discount_rate',all.x = T)
#-------------距离的重编码---Distance_woe-----------------------
revised <- merge(revised,Distance_woe,by.x = 'Distance_copy',by.y = 'kind',all.x = T)
revised <- subset(revised,select = -c(Distance_copy))
#-------------领取优惠卷是星期几的重编码---Weekday_woe-----------------------
revised <- merge(revised,Weekday_woe,by.x = 'is_weekday',by.y = 'kind',all.x = T)
#-----------特殊号码优惠卷的重编码  Coupon_woe---------------
revised_D <- data.frame(revised$Coupon_id)  
revised_D <- unique(revised_D)
revised_D <- merge(revised_D,Coupon_woe,by.x = 'revised.Coupon_id',by.y = 'kind',all.x = T)
revised_D$Coupon_woe[is.na(revised_D$Coupon_woe) == T] <- 1.12603961546
revised <- merge(revised,revised_D,by.x = 'Coupon_id',by.y = 'revised.Coupon_id',all.x = T)
######################## 两个前置周期 ###########################
#-------------------------------客户前一个周期（15~30天）领取的次数  Last_cycle_get_count----------
midd <- revised_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)%>%
  filter(Date_received >= '2016-06-01' & Date_received <= '2016-06-15')%>%
  group_by(User_id)%>%summarise( Last_cycle_get_count = n()) 
revised <- merge(revised,midd,by = 'User_id',all.x = T) 
revised$Last_cycle_get_count[is.na(revised$Last_cycle_get_count) == T] <- 0
#-------------------------------客户前一个周期（15~30天）领取并购买的次数  Last_cycle_get_buy_count----------
midd <- revised_offline%>%select(User_id,Date_received,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 2)%>%
  filter(Date_received >= '2016-06-01' & Date_received <= '2016-06-15')%>%
  group_by(User_id)%>%summarise( Last_cycle_get_buy_count = n()) 
revised <- merge(revised,midd,by = 'User_id',all.x = T) 
revised$Last_cycle_get_buy_count[is.na(revised$Last_cycle_get_buy_count) == T] <- 0
#-------------------------------客户前二个周期（31~45天）领取的次数  second_cycle_get_count----------
#3-16~3-31
midd <- revised_offline%>%select(User_id,Date_received)%>%filter(is.na(Date_received) != T)%>%
  filter(Date_received >= '2016-05-16' & Date_received <= '2016-05-31')%>%
  group_by(User_id)%>%summarise( second_cycle_get_count = n()) 
revised <- merge(revised,midd,by = 'User_id',all.x = T) 
revised$second_cycle_get_count[is.na(revised$second_cycle_get_count) == T] <- 0
#-------------------------------客户前二个周期（31~45天）领取并购买的次数  second_cycle_get_buy_count----------
midd <- revised_offline%>%select(User_id,Date_received,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 2)%>%
  filter(Date_received >= '2016-05-16' & Date_received <= '2016-05-31')%>%
  group_by(User_id)%>%summarise( second_cycle_get_buy_count = n()) 
revised <- merge(revised,midd,by = 'User_id',all.x = T) 
revised$second_cycle_get_buy_count[is.na(revised$second_cycle_get_buy_count) == T] <- 0
###########################  与间隔天数相关 ###############################
#---------------每一个客户领取优惠卷到使用优惠卷的最小、平均、最大间隔天数	avg_days,min_days,max_days-------
midd <- revised_offline%>%select(User_id,Date_received,Date)%>%group_by(User_id)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(User_id,Date_received,Date,diff)%>%group_by(User_id)%>%
  summarise(min_days = min(diff,na.rm = T),avg_days = mean(diff,na.rm = T),max_days = max(diff,na.rm = T))
revised <- merge(revised,midd_1,by.x = 'User_id',by.y = 'User_id',all.x = T)
revised$min_days[is.na(revised$min_days) == T] <- 180
revised$max_days[is.na(revised$max_days) == T] <- 180
revised$avg_days[is.na(revised$avg_days) == T] <- 180
#----------------每一种折扣的平均领购时间差	cou_avgdays cou_mindays cou_maxdays---------------
midd <- revised_offline%>%select(Discount_rate,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Discount_rate,Date_received,Date,diff)%>%group_by(Discount_rate)%>%
  summarise(cou_mindays = min(diff,na.rm = T),cou_avgdays = mean(diff,na.rm = T),
            cou_maxdays = max(diff,na.rm = T))
revised <- merge(revised,midd_1,by.x = 'Discount_rate',by.y = 'Discount_rate',all.x = T)
revised$cou_mindays[is.na(revised$cou_mindays) == T] <- 180
revised$cou_avgdays[is.na(revised$cou_avgdays) == T] <- 180
revised$cou_maxdays[is.na(revised$cou_maxdays) == T] <- 180
#----------------每一供应商的发的卷的平均领购时间差	M_avgdays M_mindays M_maxdays---------------
midd <- revised_offline%>%select(Merchant_id,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Merchant_id,Date_received,Date,diff)%>%group_by(Merchant_id)%>%
  summarise(M_mindays = min(diff,na.rm = T),M_avgdays = mean(diff,na.rm = T),M_maxdays = max(diff,na.rm = T))
revised <- merge(revised,midd_1,by.x = 'Merchant_id',by.y = 'Merchant_id',all.x = T)
revised$M_mindays[is.na(revised$M_mindays) == T] <- 180
revised$M_avgdays[is.na(revised$M_avgdays) == T] <- 180
revised$M_maxdays[is.na(revised$M_maxdays) == T] <- 180
#------------------每一特殊号码卷的平均领购时间差 -------------
midd <- revised_offline%>%select(Coupon_id,Date_received,Date)%>%
  filter(is.na(Date_received) != T & is.na(Date) != T)
midd$diff <- midd$Date - midd$Date_received
midd_1 <- midd%>%select(Coupon_id,Date_received,Date,diff)%>%group_by(Coupon_id)%>%
  summarise(coupon_mindays = min(diff,na.rm = T),coupon_avgdays = mean(diff,na.rm = T),
            coupon_maxdays = max(diff,na.rm = T))
revised <- merge(revised,midd_1,by.x = 'Coupon_id',by.y = 'Coupon_id',all.x = T)
revised$coupon_mindays[is.na(revised$coupon_mindays) == T] <- 180
revised$coupon_avgdays[is.na(revised$coupon_avgdays) == T] <- 180
revised$coupon_maxdays[is.na(revised$coupon_maxdays) == T] <- 180
#---截止该笔优惠卷领取的日期，领卷距最后一次购买间隔天数	buydays_diff1--------
#---截止该笔优惠卷领取的日期，倒数第一次购买距倒数第二次购买的间隔天数	buydays_diff2------
midd <- revised_offline%>%select(User_id,Date)%>%filter(is.na(Date) == F)%>%filter(Date <= '2016-04-30')%>%
  arrange(User_id,desc(Date))%>%group_by(User_id)  # arrange(User_id,desc(Date))注意括号
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date, collapse=','))
midd_1$vars <- substring(midd_1$items,1,21)   
midd_1 <- subset(midd_1,select = -c(items))
datename <- c('last_times','last_second')  
midd_2 <- midd_1%>%separate(vars,datename, sep = ',')                                          
midd_2$last_times <-  as.Date(midd_2$last_times,'%Y-%m-%d') 
midd_2$last_second <-  as.Date(midd_2$last_second,'%Y-%m-%d') 
revised <- merge(revised,midd_2,by = 'User_id',all.x = T)
revised$buydays_diff1 <- revised$Date_received - revised$last_times
revised$buydays_diff1[is.na(revised$buydays_diff1) == T] <- 180
revised$buydays_diff2 <- revised$last_times - revised$last_second
revised$buydays_diff2[is.na(revised$buydays_diff2) == T] <- 180
revised <- subset(revised,select = -c(last_times,last_second))
########################### 现实场景不存在的变量 #################################
#-------------------客户领取优惠卷的当日是否也领过其他优惠卷is_repeatget----repeat_count-------
#全预测日期单独提出来成为一个新集
midd <- revised%>%select(User_id,Date_received)%>%group_by(User_id)
#与之前不同，抽revised而不是revised_offline,
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] <- as.Date(midd[,i+2],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd[,2]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_repeatget','repeat_count')]
midd$repeat_count <- as.numeric(midd$repeat_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

#-带号码-----客户领取优惠卷的当日是否也领过同一号码的优惠卷is_cou_repeatget----cou_repeat_count-------
midd <- revised%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)
#与之前不同，抽revised而不是revised_offline,
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$cou_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_cou_repeatget','cou_repeat_count')]
midd$cou_repeat_count <- as.numeric(midd$cou_repeat_count)
midd <- midd[-1]
revised <- cbind(revised,midd)
#-----截止领卷当日的前15天内，该客户是否领过卷(不含当日)      is_get_before15-----------
#-----截止领卷当日的前15天内，该客户是否领几次卷(不含当日) get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过卷(不含当日)      is_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领几次卷(不含当日) get_after_count----------
midd <- subset(revised,select = c(User_id,Date_received))
midd_2 <- subset(revised,select = c(User_id,Date_received))
number <- max(table(midd$User_id))  
midd <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = 'User_id',all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+2] <- as.Date(midd[,i+2],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+2] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_get_before15','get_before15_count','is_get_after','get_after_count')]
midd$get_before15_count <- as.numeric(midd$get_before15_count)
midd$get_after_count <- as.numeric(midd$get_after_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

#-带号码---截止领卷当日的前15天内，该客户是否领过同号码卷(不含当日)      is_cou_get_before15-----------
#-带号码---截止领卷当日的前15天内，该客户是否领几次同号码卷(不含当日) cou_get_before15_count-----------
#-带号码----截止领卷当日之后到月底之前，该客户是否领过同号码卷(不含当日)      is_cou_get_after----------
#-带号码----截止领卷当日之后到月底之前，该客户是否领几次同号码卷(不含当日) cou_get_after_count----------
midd <- subset(revised,select = c(User_id,Coupon_id,Date_received))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(revised,select = c(User_id,Coupon_id,Date_received))
midd <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Coupon_id'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$cou_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_cou_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$cou_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_cou_get_before15','cou_get_before15_count',
               'is_cou_get_after','cou_get_after_count')]
midd$cou_get_before15_count <- as.numeric(midd$cou_get_before15_count)
midd$cou_get_after_count <- as.numeric(midd$cou_get_after_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

#-------------预测期内客户领卷的次数  P_count ----------------
midd <- revised%>%select(User_id,Date_received)%>%group_by(User_id)%>%summarise( P_count = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)  

#--带号码-----------预测期内客户领同号码卷的次数  cou_P_count ----------------
midd <- revised%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%summarise( cou_P_count = n())
revised <- merge(revised,midd,by = c('User_id','Coupon_id'),all.x = T) 

#------------------------------预测期内客户领卷距下一次领卷的天数（不含当日）P_afterdays----------------
midd <- revised%>%select(User_id,Date_received)%>%group_by(User_id)%>%arrange(User_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Date_received)
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] = as.Date(midd[,i+2],'%Y-%m-%d')
  a = midd[,2] - midd[,i+2]
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2][midd[,i+2] >= 0] = -31
}
midd$P_afterdays <- apply(midd[,3:(number+2)],1,max)
midd$P_afterdays <- abs(midd$P_afterdays)
midd <- midd[-c(2:(number+2))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]

#-带号码----------预测期内客户领卷距下一次领同号码卷的天数（不含当日）cou_P_afterdays----------------
midd <- revised%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%arrange(User_id,Coupon_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Coupon_id,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$cou_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$cou_P_afterdays <- abs(midd$cou_P_afterdays)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]

#----------------------预测期内客户领卷距上一次领卷的天数（不含当日）P_beforedays-------------------
midd <- revised%>%select(User_id,Date_received)%>%group_by(User_id)%>%arrange(User_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id)%>%summarise(items = paste(Date_received, collapse=','))
number <- max(table(midd$User_id))  
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Date_received)
midd <- merge(midd,midd_2,by = 'User_id',all.x = T)
for (i in 1:number){
  midd[,i+2] = as.Date(midd[,i+2],'%Y-%m-%d')
  a = midd[,2] - midd[,i+2]
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+2] = as.numeric(midd[,i+2])
  midd[,i+2][midd[,i+2] <= 0] = 31
}
midd$P_beforedays <- apply(midd[,3:(number+2)],1,min)
midd <- midd[-c(2:(number+2))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]

#--带号码-------预测期内客户领卷距上一次领同号码卷的天数（不含当日）cou_P_beforedays-------------------
midd <- revised%>%select(User_id,Coupon_id,Date_received)%>%
  group_by(User_id,Coupon_id)%>%arrange(User_id,Coupon_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Coupon_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Coupon_id,Date_received)%>%group_by(User_id,Coupon_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Coupon_id,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Coupon_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$cou_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]
################-----------追加------------####################-
#--------------供应商发过几个特殊号的卷---- M_Cou_count----
midd <- revised_offline%>%select(Merchant_id,Coupon_id)%>%filter(Coupon_id !='null')%>%
  group_by(Merchant_id,Coupon_id)%>%summarise(n=n())
midd_1 <- midd%>%select(Merchant_id,Coupon_id)%>%group_by(Merchant_id)%>%summarise(M_Cou_count=n())
revised <- merge(revised,midd_1,by = 'Merchant_id',all.x = T)
revised$M_Cou_count[is.na(revised$M_Cou_count) == T] <- 0
#-----------预测集的特殊号码卷，点该供商商所发的卷的占比cou_M_rate  ---
midd_1 <- revised_offline%>%select(Coupon_id,Date_received)%>%filter(Coupon_id !='null')%>%
  group_by(Coupon_id)%>%summarise(cou=n())
revised <- merge(revised,midd_1,by = 'Coupon_id',all.x = T)
revised$cou[is.na(revised$cou)] <- 0
midd <- revised_offline%>%select(Merchant_id,Coupon_id,Date_received)%>%filter(Coupon_id !='null')%>%
  group_by(Merchant_id)%>%summarise(n = n())
revised <- merge(revised,midd,by = 'Merchant_id',all.x = T)
revised$n[is.na(revised$n)] <- 0
revised$cou_M_rate <- revised$cou/revised$n
revised$cou_M_rate[is.na(revised$cou_M_rate)] <- 0
revised <- subset(revised,select = -c(cou,n))
#-------------- 预测期内 同一折扣领卷的次数  p_discount------------
midd <- revised%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%summarise( p_discount = n())
revised <- merge(revised,midd,by = c('User_id','Discount_rate'),all.x = T) 
#----------预测期内客户领卷距下一次领同一折扣的天数（不含当日）discount_P_afterdays----------------
midd <- revised%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%arrange(User_id,Discount_rate,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Discount_rate,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$discount_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$discount_P_afterdays <- abs(midd$discount_P_afterdays)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]
#--------预测期内客户领卷距上一次领同一折扣的天数（不含当日）discount_P_beforedays-------------------
midd <- revised%>%select(User_id,Discount_rate,Date_received)%>%
  group_by(User_id,Discount_rate)%>%arrange(User_id,Discount_rate,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Discount_rate,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$discount_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]
#-----客户领取优惠卷的当日是否也领过同一折扣的优惠卷is_discount_repeatget----discount_repeat_count
midd <- revised%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)
midd_1 <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Discount_rate'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$discount_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_discount_repeatget','discount_repeat_count')]
midd$discount_repeat_count <- as.numeric(midd$discount_repeat_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

#----截止领卷当日的前15天内，该客户是否领过同一折扣卷(不含当日)      is_discount_get_before15-----------
#----截止领卷当日的前15天内，该客户领过几次同一折扣卷(不含当日) discount_get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一折扣卷(不含当日)      is_discount_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一折扣卷(不含当日) discount_get_after_count----------
midd <- subset(revised,select = c(User_id,Discount_rate,Date_received))
midd_2 <- midd%>%select(User_id,Discount_rate,Date_received)%>%group_by(User_id,Discount_rate)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(revised,select = c(User_id,Discount_rate,Date_received))
midd <- midd%>%group_by(User_id,Discount_rate)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Discount_rate'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$discount_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_discount_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$discount_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_discount_get_before15','discount_get_before15_count',
               'is_discount_get_after','discount_get_after_count')]
midd$discount_get_before15_count <- as.numeric(midd$discount_get_before15_count)
midd$discount_get_after_count <- as.numeric(midd$discount_get_after_count)
midd <- midd[-1]
revised <- cbind(revised,midd)
###########-------------------------------#############################

#-------------- 预测期内 同一供应商领卷的次数  p_Mer------------
midd <- revised%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%summarise( p_Mer = n())
revised <- merge(revised,midd,by = c('User_id','Merchant_id'),all.x = T) 
#----------预测期内客户领卷距下一次领同一供应商卷的天数（不含当日）Mer_P_afterdays----------------
midd <- revised%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%arrange(User_id,Merchant_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Merchant_id,Date_received)
midd <- merge(midd,midd_2,by =c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] >= 0] = -31
}
midd$Mer_P_afterdays <- apply(midd[,4:(number+3)],1,max)
midd$Mer_P_afterdays <- abs(midd$Mer_P_afterdays)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]
#--------预测期内客户领卷距上一次领同一供应商的天数（不含当日）Mer_P_beforedays-------------------
midd <- revised%>%select(User_id,Merchant_id,Date_received)%>%
  group_by(User_id,Merchant_id)%>%arrange(User_id,Merchant_id,desc(Date_received))
midd <- distinct(midd,.keep_all = T)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') 
midd <- revised%>%select(User_id,Merchant_id,Date_received)
midd <- merge(midd,midd_2,by = c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] = as.Date(midd[,i+3],'%Y-%m-%d')
  a = midd[,3] - midd[,i+3]
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3] = a
}  
midd[is.na(midd) == T] <- 0
for (i in 1:number){
  midd[,i+3] = as.numeric(midd[,i+3])
  midd[,i+3][midd[,i+3] <= 0] = 31
}
midd$Mer_P_beforedays <- apply(midd[,4:(number+3)],1,min)
midd <- midd[-c(3:(number+3))]
revised <- cbind(revised,midd)
revised <- revised[-c(ncol(revised)-1)]
revised <- revised[-c(ncol(revised)-1)]
#-----客户领取优惠卷的当日是否也领过同一供应商的优惠卷is_Mer_repeatget----Mer_repeat_count
midd <- revised%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)
midd_1 <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd_2 <- midd_1%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
midd <- merge(midd,midd_2,by = c('User_id','Merchant_id'),all.x = T)
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd[,3]
  midd$diff[midd$diff == 0] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_repeatget <- ifelse(midd$is_repeat>1,1,0)
midd$Mer_repeat_count <- midd$is_repeat-1
midd <- midd[c('User_id','is_Mer_repeatget','Mer_repeat_count')]
midd$Mer_repeat_count <- as.numeric(midd$Mer_repeat_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

#----截止领卷当日的前15天内，该客户是否领过同一供应商卷(不含当日)      is_Mer_get_before15-----------
#----截止领卷当日的前15天内，该客户领过几次同一供应商卷(不含当日) Mer_get_before15_count-----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一供应商卷(不含当日)      is_Mer_get_after----------
#-----截止领卷当日之后到月底之前，该客户是否领过同一供应商卷(不含当日) Mer_get_after_count----------
midd <- subset(revised,select = c(User_id,Merchant_id,Date_received))
midd_2 <- midd%>%select(User_id,Merchant_id,Date_received)%>%group_by(User_id,Merchant_id)%>%
  summarise( count = n())
number <- max(midd_2$count)  #取出来此段时间内最大的领卷次数
midd_2 <- subset(revised,select = c(User_id,Merchant_id,Date_received))
midd <- midd%>%group_by(User_id,Merchant_id)%>%summarise(items = paste(Date_received, collapse=','))
midd <- merge(midd_2,midd,by = c('User_id','Merchant_id'),all.x = T)
midd$before <- midd$Date_received - 15
midd$after <- midd$Date_received + 15
datename <- c()   #生成一个变名序列的向量
for (i in 1:number){
  datename[i] = paste("Date",i,sep = "")
  i = i + 1
}
midd <- midd%>%separate(items,datename, sep = ',') #把所有日期拆成最大列数
for (i in 1:number){
  midd[,i+3] <- as.Date(midd[,i+3],'%Y-%m-%d')
}    
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$before
  midd$diff[midd$diff >= 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_get_before15 <- ifelse(midd$is_repeat>=1,1,0)
midd$Mer_get_before15_count <- midd$is_repeat  #含当日
midd$diff <- 0
midd$is_repeat <- 0
for (i in 1:number){
  midd$diff = midd[,i+3] - midd$Date_received
  midd$diff[midd$diff > 0 & midd$diff < 15] = 300    
  midd$diff[midd$diff != 300] = 0    #NA不算，NA不等于“不是300”
  midd$diff[midd$diff == 300] = 1
  midd$diff[is.na(midd$diff) == T] = 0
  midd$is_repeat = midd$is_repeat + midd$diff
  i = i + 1
}
midd$is_Mer_get_after <- ifelse(midd$is_repeat>=1,1,0)
midd$Mer_get_after_count <- midd$is_repeat
midd <- midd[c('User_id','is_Mer_get_before15','Mer_get_before15_count',
               'is_Mer_get_after','Mer_get_after_count')]
midd$Mer_get_before15_count <- as.numeric(midd$Mer_get_before15_count)
midd$Mer_get_after_count <- as.numeric(midd$Mer_get_after_count)
midd <- midd[-1]
revised <- cbind(revised,midd)

write.csv(revised,"E:/R 3.4.2 for Windows/O2O_tc/revised_two.csv")
#---------------------------- 预测期内客户是第几次领卷  rank ---------------------------
#SQL重排序报错，只能写完再读进来
index<-function(x){return(c(1:length(x)))}
revised <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/revised_two.csv")
revised <- revised[-1]
revised <- sqldf("select * from revised order by User_id,Date_received")
revised$User_id <- as.factor(revised$User_id)
revised <-transform(revised, group = as.integer(User_id))
revised1 <- revised[1:50000,]
revised1 <- transform(revised1,rank = unlist(tapply(Date_received,group,index)))
revised2 <- revised[50001:113640,]
revised2 <- transform(revised2,rank = unlist(tapply(Date_received,group,index)))

revised <- rbind(revised1,revised2)
revised <- subset(revised,select = -c(group))
rm(revised1,revised2)

write.csv(revised,"E:/R 3.4.2 for Windows/O2O_tc/revised_two.csv")

rm(Coupon_woe,Discount_woe,Distance_woe,Merchant_woe)
rm(midd,midd_1,midd_2,revised_D,revised_M,revised_offline)
rm(train_offline,Weekday_woe,a,datename,i,number,grepFun,index)
