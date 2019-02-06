###############线上集的处理######## Train & Revised ####################################
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
online <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/ccf_online_stage1_train.csv")
######## 数据格式转化 ###########
online$User_id <- as.character(online$User_id)
online$Merchant_id <- as.character(online$Merchant_id)
online$Coupon_id <- as.character((online$Coupon_id))
online$Discount_rate <- as.character(online$Discount_rate)

online$Date_received <- as.character(online$Date_received)
online$Date_received <- as.Date(online$Date_received,'%Y%m%d')
online$Date <- as.character(online$Date)
online$Date <- as.Date(online$Date,'%Y%m%d')
online$weekday_r <- weekdays(online$Date_received)
######## 打标识 ############
online$Discount_fac <- NA
online$Discount_fac[grepl("^[z0-9]{1,4}\\:[z0-9]{1,4}$",online$Discount_rate) == T] <- 1
online$Discount_fac[online$Discount_rate == "null"] <- 2
online$Discount_fac[online$Discount_rate == "fixed"] <- 3
#1：满减，2：什么也不用的普通消费3：限时消费
online$buy_fac <- NA
online$buy_fac[is.na(online$Date) == T & is.na(online$Date_received) == F] <- 2 
online$buy_fac[is.na(online$Date) == F & online$Discount_rate == 'null' ] <- 3
online$buy_fac[is.na(online$Date) == F & online$Discount_rate == 'fixed' ] <- 4
online$buy_fac[is.na(online$Date) == F & online$Discount_rate != 'null' & 
                 online$Discount_rate != 'fixed'] <- 1
# 1:领卷已消费 2:领卷未消费，3:什么也不用的消4：限时消费（也领卷）
######## 分集 #########
online <- tbl_df(online)
train_online_1 <- filter(online,Date>='2016-01-01' & Date<='2016-04-30' & 
                            Date_received>='2016-01-01' & Date_received<='2016-04-30')
train_online_2 <- filter(online,Date_received>='2016-01-01' & Date_received<='2016-04-30',is.na(Date))
train_online_3 <- filter(online,Date>='2016-01-01' & Date<='2016-04-30',is.na(Date_received))
train_online <- rbind(train_online_1,train_online_2,train_online_3)
print(c(min(train_online$Date_received,na.rm = T), max(train_online$Date_received,na.rm = T)))
print(c(min(train_online$Date,na.rm = T), max(train_online$Date,na.rm = T))) 
train <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/train_two.csv")
train <- train[-1]
revised_online_1 <- filter(online,Date>='2016-02-16' & Date<='2016-06-15' & 
                              Date_received>='2016-02-16' & Date_received<='2016-06-15')
revised_online_2 <- filter(online,Date_received>='2016-02-16' & Date_received<='2016-06-15',is.na(Date))
revised_online_3 <- filter(online,Date>='2016-02-16' & Date<='2016-06-15',is.na(Date_received))
revised_online <- rbind(revised_online_1,revised_online_2,revised_online_3)
print(c(min(revised_online$Date_received,na.rm = T), max(revised_online$Date_received,na.rm = T)))
print(c(min(revised_online$Date,na.rm = T), max(revised_online$Date,na.rm = T)))
revised <- read.csv("E:/R 3.4.2 for Windows/O2O_tc/revised_two.csv")
revised <- revised[-1]
rm(revised_online_1,revised_online_2,revised_online_3)
rm(train_online_1,train_online_2,train_online_3)
rm(online)
gc()
##################线上处理开始###################
#---客户领取  非限时 卷次数（线上）get_count_on_notfixed----    
midd <- train_online%>%select(User_id,Coupon_id)%>%
  filter(Coupon_id != 'null'&Coupon_id != 'fixed')%>%
  group_by(User_id)%>%summarise(get_count_on_notfixed=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_count_on_notfixed[is.na(train$get_count_on_notfixed) == T] <- 0
#----客户领取非限时优惠卷未购买的次数（线上）get_no_buy_on----
midd <- train_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id)%>%summarise(get_no_buy_on=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_no_buy_on[is.na(train$get_no_buy_on) == T] <- 0
#---客户领取  限时 卷次数（线上）get_count_on_fixed----    
midd <- train_online%>%select(User_id,Coupon_id)%>%filter(Coupon_id == 'fixed')%>%
  group_by(User_id)%>%summarise(get_count_on_fixed=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_count_on_fixed[is.na(train$get_count_on_fixed) == T] <- 0
#----客户领取优惠卷并且已经购买的次数（线上）get_buy_on(含fixed)-----------------
midd <- train_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 4)%>%
  group_by(User_id)%>%summarise(get_buy_on=n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$get_buy_on[is.na(train$get_buy_on) == T] <- 0
#-------------客户什么卷也不用的普通购买次数（线上）nodiscount_buy_on--------
midd <- train_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id)%>%summarise(nodiscount_buy_on = n())
train <- merge(train,midd,by = 'User_id',all.x = T)
train$nodiscount_buy_on[is.na(train$nodiscount_buy_on) == T] <- 0
#------------ 客户在线上领卷的比重 -- 线上领卷/(线上领卷+线下领卷) get_count_rate_on ------
train$get_count_rate_on <- (train$get_count_on_notfixed + train$get_count_on_fixed) / 
  (train$get_count_on_notfixed + train$get_count_on_fixed + train$get_count_off)
train$get_count_rate_on[is.na(train$get_count_rate_on)] <- 0
#------------ 客户在线上领卷购买的比重 -- 线上购买 /(线上购买+线下购买) get_buy_rate_on------
train$get_buy_rate_on <- train$get_buy_on / (train$get_buy_on + train$get_buy_off)
train$get_buy_rate_on[is.na(train$get_buy_rate_on)] <- 0
#------------ 客户在线上所有购买的比重 -- 线上购买 /(线上购买+线下购买) get_buy_rate_on_all------
train$get_buy_rate_on_all <- (train$get_buy_on + train$nodiscount_buy_on)/
  (train$get_buy_on + train$nodiscount_buy_on + train$get_buy_off + train$nodiscount_buy_off)
train$get_buy_rate_on_all[is.na(train$get_buy_rate_on_all)] <- 0
#------------ 客户 线上领卷未购买的比重 -- 线上领卷未购买次数 /(线上+线下)get_no_buy_rate_on------
train$get_no_buy_rate_on <- train$get_no_buy_on / (train$get_no_buy_on + train$get_no_buy_off)
train$get_no_buy_rate_on[is.na(train$get_no_buy_rate_on)] <- 0
#----历史上 该客户是否只在 线上购买 is_all_on_buy --------
train$is_all_on_buy <- ifelse((train$get_buy_on+train$nodiscount_buy_on)!= 0 & 
                                (train$nodiscount_buy_off+train$get_buy_off)== 0,1,0)
#----历史上 该客户是否只在 线下购买 is_all_off_buy --------
train$is_all_off_buy <- ifelse((train$get_buy_on+train$nodiscount_buy_on)== 0 & 
                                (train$nodiscount_buy_off+train$get_buy_off)!= 0,1,0)
#----历史上 该客户是否 线下线下都没购买过 is_all_not_buy --------
train$is_all_not_buy <- ifelse((train$get_buy_on+train$nodiscount_buy_on) == 0 & 
                                (train$nodiscount_buy_off+train$get_buy_off)== 0,1,0)
#----历史上 该客户是否 线下线下都买过 is_all_buy --------
train$is_all_buy <- ifelse((train$get_buy_on+train$nodiscount_buy_on) != 0 & 
                                 (train$nodiscount_buy_off+train$get_buy_off)!= 0,1,0)
#----历史上 该客户是否线上只是NULL购买 is_null_buy_on --------
train$is_null_buy_on <- ifelse(train$nodiscount_buy_on != 0 & train$get_buy_on ==0,1,0 )
#----历史上 该客户是否线下只是NULL购买 is_null_buy_off --------
train$is_null_buy_off <- ifelse(train$nodiscount_buy_off != 0 & train$get_buy_off ==0,1,0 )
#----历史上 该客户不管线上还是线下都是NULL购买 is_null_buy_all --------
train$is_null_buy_all <- ifelse( train$is_null_buy_on == 1 & train$is_null_buy_off == 1,1,0 )
#----历史上 该客户是否只是FIXED购买 is_fixed_buy --------
train$is_fixed_buy <- ifelse((train$get_buy_on -train$get_count_on_fixed + train$nodiscount_buy_on) == 0 & 
                               train$get_count_on_fixed != 0 & 
                               (train$nodiscount_buy_off+train$get_buy_off)== 0,1,0) 
##----历史上 线上领卷从未购买过 is_all_getnobuy_on --------
train$is_all_getnobuy_on <- ifelse(train$get_buy_on == 0,1,0)
##----历史上 线下领卷从未购买过 is_all_getnobuy_off --------
train$is_all_getnobuy_off <- ifelse(train$get_buy_off == 0,1,0)
##----历史上 上线下领卷都从未购买过 is_all_getnobuy--------
train$is_all_getnobuy <- ifelse(train$get_buy_on == 0 & train$get_buy_off == 0,1,0)
#-----当期的折扣，是否在线上购过 Discount_is_buy_on----
midd <- train_online%>%select(User_id,Discount_rate,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)  
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Discount_rate)%>%group_by(User_id)%>%
  summarise(items = paste(Discount_rate, collapse=',') )   
train <- merge(train,midd,by = 'User_id',all.x = T)
grepFun <- function(train){
  grepl(train['Discount_rate'],train['items'],fixed=TRUE)
}                                                
train$Discount_is_buy_on <- apply(train,1,grepFun)
train$Discount_is_buy_on[train$Discount_is_buy_on == TRUE] <- 1
train$Discount_is_buy_on[train$Discount_is_buy_on == FALSE] <- 0
train <- subset(train,select = -c(items))  
rm(grepFun,train_online,midd)
#=====================================

#---客户领取  非限时 卷次数（线上）get_count_on_notfixed----    
midd <- revised_online%>%select(User_id,Coupon_id)%>%
  filter(Coupon_id != 'null'&Coupon_id != 'fixed')%>%
  group_by(User_id)%>%summarise(get_count_on_notfixed=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_count_on_notfixed[is.na(revised$get_count_on_notfixed) == T] <- 0
#----客户领取非限时优惠卷未购买的次数（线上）get_no_buy_on----
midd <- revised_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 2)%>%
  group_by(User_id)%>%summarise(get_no_buy_on=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_no_buy_on[is.na(revised$get_no_buy_on) == T] <- 0
#---客户领取  限时 卷次数（线上）get_count_on_fixed----    
midd <- revised_online%>%select(User_id,Coupon_id)%>%filter(Coupon_id == 'fixed')%>%
  group_by(User_id)%>%summarise(get_count_on_fixed=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_count_on_fixed[is.na(revised$get_count_on_fixed) == T] <- 0
#----客户领取优惠卷并且已经购买的次数（线上）get_buy_on(含fixed)-----------------
midd <- revised_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 1 | buy_fac == 4)%>%
  group_by(User_id)%>%summarise(get_buy_on=n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$get_buy_on[is.na(revised$get_buy_on) == T] <- 0
#-------------客户什么卷也不用的普通购买次数（线上）nodiscount_buy_on--------
midd <- revised_online%>%select(User_id,buy_fac)%>%filter(buy_fac == 3)%>%
  group_by(User_id)%>%summarise(nodiscount_buy_on = n())
revised <- merge(revised,midd,by = 'User_id',all.x = T)
revised$nodiscount_buy_on[is.na(revised$nodiscount_buy_on) == T] <- 0
#------------ 客户在线上领卷的比重 -- 线上领卷/(线上领卷+线下领卷) get_count_rate_on ------
revised$get_count_rate_on <- (revised$get_count_on_notfixed + revised$get_count_on_fixed) / 
  (revised$get_count_on_notfixed + revised$get_count_on_fixed + revised$get_count_off)
revised$get_count_rate_on[is.na(revised$get_count_rate_on)] <- 0
#------------ 客户在线上领卷购买的比重 -- 线上购买 /(线上购买+线下购买) get_buy_rate_on------
revised$get_buy_rate_on <- revised$get_buy_on / (revised$get_buy_on + revised$get_buy_off)
revised$get_buy_rate_on[is.na(revised$get_buy_rate_on)] <- 0
#------------ 客户在线上所有购买的比重 -- 线上购买 /(线上购买+线下购买) get_buy_rate_on_all------
revised$get_buy_rate_on_all <- (revised$get_buy_on + revised$nodiscount_buy_on)/
  (revised$get_buy_on + revised$nodiscount_buy_on + revised$get_buy_off + revised$nodiscount_buy_off)
revised$get_buy_rate_on_all[is.na(revised$get_buy_rate_on_all)] <- 0
#------------ 客户 线上领卷未购买的比重 -- 线上领卷未购买次数 /(线上+线下)get_no_buy_rate_on------
revised$get_no_buy_rate_on <- revised$get_no_buy_on / (revised$get_no_buy_on + revised$get_no_buy_off)
revised$get_no_buy_rate_on[is.na(revised$get_no_buy_rate_on)] <- 0
#----历史上 该客户是否只在 线上购买 is_all_on_buy --------
revised$is_all_on_buy <- ifelse((revised$get_buy_on+revised$nodiscount_buy_on)!= 0 & 
                                (revised$nodiscount_buy_off+revised$get_buy_off)== 0,1,0)
#----历史上 该客户是否只在 线下购买 is_all_off_buy --------
revised$is_all_off_buy <- ifelse((revised$get_buy_on+revised$nodiscount_buy_on)== 0 & 
                                 (revised$nodiscount_buy_off+revised$get_buy_off)!= 0,1,0)
#----历史上 该客户是否 线下线下都没购买过 is_all_not_buy --------
revised$is_all_not_buy <- ifelse((revised$get_buy_on+revised$nodiscount_buy_on) == 0 & 
                                 (revised$nodiscount_buy_off+revised$get_buy_off)== 0,1,0)
#----历史上 该客户是否 线下线下都买过 is_all_buy --------
revised$is_all_buy <- ifelse((revised$get_buy_on+revised$nodiscount_buy_on) != 0 & 
                             (revised$nodiscount_buy_off+revised$get_buy_off)!= 0,1,0)


#----历史上 该客户是否线上只是NULL购买 is_null_buy_on --------
revised$is_null_buy_on <- ifelse(revised$nodiscount_buy_on != 0 & revised$get_buy_on ==0,1,0 )
#----历史上 该客户是否线下只是NULL购买 is_null_buy_off --------
revised$is_null_buy_off <- ifelse(revised$nodiscount_buy_off != 0 & revised$get_buy_off ==0,1,0 )
#----历史上 该客户不管线上还是线下都是NULL购买 is_null_buy_all --------
revised$is_null_buy_all <- ifelse( revised$is_null_buy_on == 1 & revised$is_null_buy_off == 1,1,0 )
#----历史上 该客户是否只是FIXED购买 is_fixed_buy --------
revised$is_fixed_buy <- ifelse((revised$get_buy_on -revised$get_count_on_fixed + revised$nodiscount_buy_on) == 0 & 
                               revised$get_count_on_fixed != 0 & 
                               (revised$nodiscount_buy_off+revised$get_buy_off)== 0,1,0) 
##----历史上 线上领卷从未购买过 is_all_getnobuy_on --------
revised$is_all_getnobuy_on <- ifelse(revised$get_buy_on == 0,1,0)
##----历史上 线下领卷从未购买过 is_all_getnobuy_off --------
revised$is_all_getnobuy_off <- ifelse(revised$get_buy_off == 0,1,0)
##----历史上 上线下领卷都从未购买过 is_all_getnobuy--------
revised$is_all_getnobuy <- ifelse(revised$get_buy_on == 0 & revised$get_buy_off == 0,1,0)
#-----当期的折扣，是否在线上购过 Discount_is_buy_on----
midd <- revised_online%>%select(User_id,Discount_rate,buy_fac)%>%
  filter(buy_fac == 1)%>%group_by(User_id)  
midd <- midd[c(1:2)]
midd <- distinct(midd,.keep_all = T)
midd <- midd%>%select(User_id,Discount_rate)%>%group_by(User_id)%>%
  summarise(items = paste(Discount_rate, collapse=',') )   
revised <- merge(revised,midd,by = 'User_id',all.x = T)
grepFun <- function(revised){
  grepl(revised['Discount_rate'],revised['items'],fixed=TRUE)
}                                                
revised$Discount_is_buy_on <- apply(revised,1,grepFun)
revised$Discount_is_buy_on[revised$Discount_is_buy_on == TRUE] <- 1
revised$Discount_is_buy_on[revised$Discount_is_buy_on == FALSE] <- 0
revised <- subset(revised,select = -c(items))  
rm(grepFun,revised_online,midd)
write.csv(revised,"E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/revised_two_1.csv")
write.csv(train,"E:/R 3.4.2 for Windows/O2O_tc/2019.1.19/train_two_1.csv")
