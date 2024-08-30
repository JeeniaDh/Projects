

library(dplyr)
library(lubridate)
#load data
getwd()
setwd("/Users/chenhuiguo/Desktop/MSU Project Purchase to Pay/Project Data")

# 1.1 load data
EKPO=read.csv("EKPO.csv",colClasses=c(EBELN="character",EBELP="character"),header=TRUE,sep=",")
EKKO=read.csv("EKKO.csv",colClasses=c(EBELN="character"),header=TRUE,sep=",")
EKBE=read.csv("EKBE.csv",colClasses=c(EBELN="character",EBELP="character"),header=TRUE,sep=",")
EBAN=read.csv("EBAN.csv",colClasses=c(EBELN="character",EBELP="character"),header=TRUE,sep=",")
Activity=read.csv("Activity.csv",colClasses=c(X_CASE_KEY="character",EBELN="character",EBELP="character"),header=TRUE,sep=",")


# 1.2. cleanup delivery data
View(Activity)
str(Activity)
Activity=Activity[,!(colnames(Activity) %in% c("MANDT"))]
Activity=Activity%>%select(-starts_with("CHANGED"))

Activity$time=strptime(Activity$EVENTTIME,"%b %d %Y %I:%M %p")

Activity=Activity%>%arrange(EBELN,EBELP,time)%>%
            group_by(X_CASE_KEY,EBELN,EBELP)%>%
            mutate(create=if_else(ACTIVITY_EN=="Create Purchase Order Item",1,0),
                  receive=if_else(ACTIVITY_EN=="Record Goods Receipt",1,0),
                  changeconfirmeddeliverydate=if_else(ACTIVITY_EN=="Change Confirmed Delivery Date",1,0),
                  changecontract=if_else(ACTIVITY_EN=="Change Contract",1,0),
                  changecurrency=if_else(ACTIVITY_EN=="Change Currency",1,0),
                  changedeliveryindicator=if_else(ACTIVITY_EN=="Change Delivery Indicator",1,0),
                  changefinalinvoiceindicator=if_else(ACTIVITY_EN=="Change Final Invoice Indicator",1,0),
                  changeoutwarddeliveryindicator=if_else(ACTIVITY_EN=="Change Outward Delivery Indicator",1,0),
                  changeprice=if_else(ACTIVITY_EN=="Change Price",1,0),
                  changequantity=if_else(ACTIVITY_EN=="Change Quantity",1,0),
                  changerequesteddeliverydate=if_else(ACTIVITY_EN=="Change requested delivery date",1,0),
                  changestoragelocation=if_else(ACTIVITY_EN=="Change Storage Location",1,0))%>%
            mutate(receive=cumsum(receive)*receive) #use 1,2,3 to index the record goods receipt events

ActivitySummary=Activity%>%group_by(X_CASE_KEY,EBELN,EBELP)%>%
                   summarize(
                   #get the number of each type of change-item events 
                   changeconfirmeddeliverydate=sum(changeconfirmeddeliverydate), 
                   changecontract=sum(changecontract),
                   changecurrency=sum(changecurrency),
                   changedeliveryindicator=sum(changedeliveryindicator),
                   changefinalinvoiceindicator=sum(changefinalinvoiceindicator),
                   changeoutwarddeliveryindicator=sum(changeoutwarddeliveryindicator),
                   changeprice=sum(changeprice),
                   changequantity=sum(changequantity),
                   changerequesteddeliverydate=sum(changerequesteddeliverydate),
                   changestoragelocation=sum(changestoragelocation),
                   numdelivery=max(receive))

createTime=Activity%>%filter(create==1)%>%
      select(X_CASE_KEY,EBELN,EBELP,time)%>%
      rename(createtime=time)
#86402 cases with create purchase
dim(createTime)
  
firstReceiveTime=Activity%>%filter(receive==1)%>%
      select(X_CASE_KEY,EBELN,EBELP,time)%>%
      rename(firstreceivetime=time)
#76412 cases with first receive goods
dim(firstReceiveTime)

#get GD days with 2 decimal digits
options(scipen = 999)
data=createTime%>%inner_join(firstReceiveTime,by=c("X_CASE_KEY","EBELN","EBELP"))%>%
                  inner_join(ActivitySummary,by=c("X_CASE_KEY","EBELN","EBELP"))%>%
                  mutate(GDdays=round(100*as.numeric(difftime(firstreceivetime,createtime,units="days")))/100)

# clean EKPO table
EKPO=EKPO[,(colnames(EKPO) %in% c("NETPR","EBELP","EBELN","BUKRS",
                                  "MATNR","MATKL","LOEKZ","PSTYP","WERKS"))]
# clean EBAN table
EBAN=EBAN[,(colnames(EBAN) %in% c("EBELP","EBELN","ERNAM"))]
#remove duplicated records (some records are the same except the BADAT column)
EBAN=EBAN[!(duplicated(EBAN)),]


#join tables
data=data%>%left_join(EKPO,by=c("EBELP","EBELN"))%>%
            left_join(EBAN,by=c("EBELP","EBELN"))



# 2.1.load and clean Payment data
BSEG=read.csv("BSEG.csv",colClasses=c(GJAHR="character",BELNR="character"),header=TRUE,sep=",")
BKPF=read.csv("BKPF.csv",colClasses=c(GJAHR="character",BELNR="character",RSEG_GJAHR="character",RSEG_BELNR="character",RBKPKEY="character"),header=TRUE,sep=",")
RBKP=read.csv("RBKP.csv",colClasses=c(GJAHR="character",BELNR="character"),header=TRUE,sep=",")
RSEG=read.csv("RSEG.csv",colClasses=c(GJAHR="character",BELNR="character",EBELN="character",EBELP="character"),header=TRUE,sep=",")

#remove duplicated columns, missing value columns, and useless column MANDT
BSEG=BSEG[,!(colnames(BSEG) %in% c("MANDT"))]
BKPF=BKPF[,!(colnames(BKPF) %in% c("MANDT","BUDAT","CPUDT","XBLNR","BLART","BLDAT"))]
RBKP=RBKP[,!(colnames(RBKP) %in% c("MANDT","BUDAT","CPUDT","BLDAT","ZFBDT","FDTAG",
                                   "REINDAT","VATDATE","ASSIGN_NEXT_DATE","ASSIGN_END_DATE"))]
RSEG=RSEG[,!(colnames(RSEG) %in% c("MANDT","RETDUEDT","BUZEI"))]

#there are some missing values for paytime, since not paid yet
BSEG$invoicetime=strptime(BSEG$BLDAT,"%b %d %Y %I:%M %p")
BSEG$paytime=strptime(BSEG$AUGDT,"%b %d %Y %I:%M %p")
BSEG$after=difftime(BSEG$paytime,BSEG$invoicetime,units="days")
BSEG$earlydays=round(100*(BSEG$ZBD1T-BSEG$after))/100
BSEG$isearly=if_else(BSEG$earlydays>0,1,0)

# 2.2.create the joined Payment-GD data
#it seems that there is a many-to-many relationship between (early payment) table BSEG and (good delivery) data we derived before
#RSEG table is the bridge between early payment data (PK is RSEG_GJAHR+RSEG_BELNR, not GJAHR+BELNR) and good delivery data (PK is EBELN+EBELP)
#Here, we rename the GJAHR and BELNR in RBKP and RSEG tables, so that the tables can be joined
RSEG=RSEG%>%rename(RSEG_GJAHR=GJAHR,RSEG_BELNR=BELNR)
#RBKP=RBKP%>%rename(RSEG_GJAHR=GJAHR,RSEG_BELNR=BELNR)
#actually, we find that RBKP table is useless, we can join BKPB directly to RSEG
#RSEG data has duplicated records, so remove duplicated records
RSEG=RSEG[!duplicated(RSEG),]
table=BSEG%>%inner_join(BKPF,by=c("GJAHR","BELNR","BUKRS"))%>%
             select(-GJAHR,-BELNR)

join=table%>%inner_join(RSEG,by=c("RSEG_GJAHR","RSEG_BELNR"),multiple="all")

#get the combined data, then we need to aggregate it into RSEG_BELNR+RSEG_GJAHR level
#we can see that RSEG_BELNR+RSEG_GJAHR=RBKPKEY, so we can use RBKPKEY as the PK of the aggregate data
#we drop all the columns in table and only keep P Key and F Keys
join=join[,(colnames(join) %in% c("RBKPKEY","EBELN","EBELP"))]

#test relationship between payment and delivery data
#aggregate data at the payment level, we find 19.2% of payments have multiple items purchased
rs1=join%>%group_by(RBKPKEY)%>%
  summarize(count=n())%>%
  arrange(desc(count))
mean(rs1$count>1)
#[1] 0.1919914
#aggregate data at the GD delivery data, we find 9.2% of purchases have multiple payments
rs2=join%>%group_by(EBELN,EBELP)%>%
              summarize(count=n())%>%
              arrange(desc(count))
mean(rs2$count>1)
#[1] 0.09174381

# 2.3. make the Payment data
#To get payment data, since each payment record may have multiple deliveries, we
#aggregate the joint data at the payment level, by aggregating the GD variables (mean, sum, first, or count)
#for each RBKPKEY, we find there is exactly one BUKRS, so use BUKRS as aggregate variable
combined=join%>%inner_join(data,by=c("EBELN","EBELP"))
aggregated=combined%>%group_by(RBKPKEY,BUKRS)%>%
                      arrange(RBKPKEY,createtime)%>%
                      summarize(numitems=n(),
                                mincreatetime=min(createtime),
                                minfirstreceivetime=min(firstreceivetime),
                                avgGDdays=mean(GDdays,na.rm=TRUE),
                                sumNETPR=sum(NETPR,na.rm=TRUE),
                                sumchangeconfirmeddeliverydate=sum(changeconfirmeddeliverydate), 
                                sumchangecontract=sum(changecontract),
                                sumchangecurrency=sum(changecurrency),
                                sumchangedeliveryindicator=sum(changedeliveryindicator),
                                sumchangefinalinvoiceindicator=sum(changefinalinvoiceindicator),
                                sumchangeoutwarddeliveryindicator=sum(changeoutwarddeliveryindicator),
                                sumchangeprice=sum(changeprice),
                                sumchangequantity=sum(changequantity),
                                sumchangerequesteddeliverydate=sum(changerequesteddeliverydate),
                                sumchangestoragelocation=sum(changestoragelocation),
                                dcountWERKS=n_distinct(WERKS),
                                dcountMATKL=n_distinct(MATKL),
                                dcountERNAM=n_distinct(ERNAM),
                                posPSTYP=if_else(sum(PSTYP)>0,1,0),
                                sumnumdelivery=sum(numdelivery))

#test distributions
table(aggregated$dcountMATKL)
table(aggregated$dcountWERKS)
table(aggregated$dcountERNAM)
table(aggregated$posPSTYP)
  
#join the aggregated data with the payment table (BSEG)
#for same payment records, we cannot find matched delivery data; maybe the product is not delivered yet
pdata=table%>%inner_join(aggregated,by=c("RBKPKEY","BUKRS"))


# output GD data
write.table(data,"GData.csv",sep=",",row.names=FALSE,col.names=TRUE)

# output payment data
write.table(pdata,"PData.csv",sep=",",row.names=FALSE,col.names=TRUE)


# 3.1 logit model for pdata
#re-code Company Code variable (change some very rare company codes to 0)
#use 0 to fill in missing value (no payment at all) of isearly
#aggregate change events
pdata=pdata%>%mutate(BUKRS=if_else(BUKRS %in% c("CompanyCode12","CompanyCode13","CompanyCode15","CompanyCode28",
                                                "CompanyCode6","CompanyCode24","CompanyCode27","CompanyCode17"),"CompanyCode0",BUKRS))%>%
             mutate(isearly=if_else(is.na(isearly),0,isearly))%>%
             mutate(sumchange=sumchangeconfirmeddeliverydate+sumchangecontract+sumchangecurrency+sumchangedeliveryindicator+sumchangefinalinvoiceindicator+
                              sumchangeoutwarddeliveryindicator+sumchangeprice+sumchangequantity+sumchangerequesteddeliverydate+sumchangestoragelocation)

#create data variables based on invoice time
pdata$year=year(pdata$invoicetime)
table(pdata$year)
#there is only one record in year 2020, drop that record
pdata=pdata%>%filter(year>2020)
pdata$month=month(pdata$invoicetime)
table(pdata$month)
pdata$dayofweek=weekdays(pdata$invoicetime)
table(pdata$dayofweek)

#calculate the accuracy of a naive model (any model with accuracy lower than this number is useless)
max(table(pdata$isearly))/nrow(pdata)  
#[1] 0.7793202

#cross-validated logit model
set.seed(3)
#we use a quadratic model for continuous month (to model potential non-linear seasonal pattern)
logit1=glm(isearly~BUKRS+ZBD1T+SHKZG+year+month+I(month^2)+dayofweek+I(log(numitems))+I(log(1+dcountMATKL))+I(log(1+dcountERNAM))+I(log(1+sumchange))+sumNETPR,data=pdata,family=binomial(link="logit"))
summary(logit1)
cost.accuracy=function(r, pi = 0) mean(abs(r-pi)<0.5)
cv.accuracy=boot::cv.glm(pdata,logit1,cost=cost.accuracy,K=5)
cv.accuracy$delta[1]

#training errors in confusion matrix
pdata$prob=predict(logit1,pdata,type="response")
pdata$pred=ifelse(pdata$prob>0.5,1,0)
confusion=table(actual=pdata$isearly,predicted=pdata$pred)
confusion

#draw ROC curve
library(pROC)
roc_obj=roc(pdata$isearly,pdata$prob,drop=TRUE,n=100)
minusspec=1-roc_obj$specificities
plot(minusspec,roc_obj$sensitivities,main="ROC Curve",col="blue",lwd=2,xlab="1-Specificity",ylab="Sensitivity",type="l")
lines(c(0,1),c(0,1),col="red", lty=2,lwd=2)
legend("bottomright", legend=c("ROC Curve", "Random"), col=c("blue", "red"), lwd=2, lty=c(1, 2))
roc_obj$auc


# 3.2 linear model for gdata
#re-code Company Code variable (change some very rare company codes to 0)
#use 0 to fill in missing value (no payment at all) of isearly
#aggregate change events
#for PSTYP, we combine the values 1,3,5 into -1, so that we can use it as categorical variable
data=data%>%mutate(BUKRS=if_else(BUKRS %in% c("CompanyCode11","CompanyCode12","CompanyCode13","CompanyCode15","CompanyCode28",
                                              "CompanyCode31","CompanyCode32","CompanyCode33","CompanyCode34","CompanyCode6"),"CompanyCode0",BUKRS))%>%
           mutate(change=changeconfirmeddeliverydate+changecontract+changecurrency+changedeliveryindicator+changefinalinvoiceindicator+
                         changeoutwarddeliveryindicator+changeprice+changequantity+changerequesteddeliverydate+changestoragelocation)%>%
           mutate(PSTYP=if_else(PSTYP %in% c(1,3,5),-1,PSTYP))%>%
           filter(!is.na(GDdays))

data$year=year(data$createtime)
#there is only one year, so we should not use it as a predictor
table(data$year)
data$month=month(data$createtime)
#there are only three months
table(data$month)
data$dayofweek=weekdays(data$createtime)
table(data$dayofweek)

#log-transformation for GDdays, +1 to avoid log(0)
data$logGDdays=log(1+data$GDdays)



#try linear model
linear1=lm(logGDdays~BUKRS+factor(month)+dayofweek+I(1-is.na(ERNAM))+I(log(1+change))+I(log(1+NETPR))+factor(PSTYP),data=data)
summary(linear1) 


#cross-validated logit model
set.seed(3)
#we create month as categorical variable because there are only 3 months
linear1=glm(logGDdays~BUKRS+factor(month)+dayofweek+I(1-is.na(ERNAM))+I(log(1+change))+I(log(1+NETPR))+factor(PSTYP),data=data,family=gaussian)
summary(linear1)

cost.MSE=function(r, p) mean((p-r)^2)
cost.RMSE=function(r, p) sqrt(mean((p-r)^2))
cost.MAE=function(r, p) mean(abs(p-r))
cv.MSE=boot::cv.glm(data,linear1,cost=cost.MSE,K=5)
cv.RMSE=boot::cv.glm(data,linear1,cost=cost.RMSE,K=5)
cv.MAE=boot::cv.glm(data,linear1,cost=cost.MAE,K=5)
c(cv.MSE$delta[1],cv.RMSE$delta[1],cv.MAE$delta[1])

