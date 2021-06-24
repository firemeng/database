s.dir.data.daily
# 数据的三个来源
s.dir.database_handmade <- "D:/handmade_data/"
s.dir.database_tonglian <- "D:/tonglian_data/"
s.dir.database_wind <- "D:/wind_data/"

d.date_begin <- tail(v.dates_all,1) + 1
d.date_end   <- as.Date("2021-04-26")

# >= d.date_begin  and <= d.date_end

library(WindR)
w.start()
# 更新date
v.dates_all_new <- v.dates_all
w_tdays_data<-w.tdays(as.character(d.date_begin),as.character(d.date_end))$Data$DATETIME
v.dates_all_new[length(v.dates_all_new)+seq(1,length(w_tdays_data),1)] <- as.Date(w_tdays_data)
tail(v.dates_all_new,5)
df.date_new <- data.frame(date=v.dates_all_new,stringsAsFactors = F)
fwrite(df.date_new, paste(s.dir.database_wind,"date.csv",sep=""), row.names = F,na="NA")
fwrite(df.date_new, paste(s.dir.data.daily,"date.csv",sep=""),row.names = F,na="NA")
# 更新ticker
# wind出问题了
w_wset_data<-w.wset('newstockissueinfo',paste('startdate=',d.date_begin,';enddate=',d.date_end,';datetype=listing;board=0;field=wind_code,sec_name,ipo_board,listing_date',sep=""))$Data

df.ticker_info_new <- data.frame(matrix(NA,nrow=0,ncol=2),stringsAsFactors = F)
colnames(df.ticker_info_new) <- colnames(df.ticker_info)
if(dim(w_wset_data)[1])
{
  for(i in dim(w_wset_data)[1]:1)
  {
    if (w_wset_data[i,"ipo_board"]!="科创板" & w_wset_data[i,"ipo_board"]!="三板")
    {
      s.ticker <- w_wset_data[i,"wind_code"]
      print(s.ticker)
      df.ticker_info_new[s.ticker,1] <- as.character(as.Date(w_wset_data[i,"listing_date"],origin = as.Date("1899-12-30")))
      df.ticker_info_new[s.ticker,2] <- as.character(as.Date(w_wset_data[i,"listing_date"],origin = as.Date("1899-12-30")))
    }
  }
}
df.ticker_info_new[,1] <- as.Date(df.ticker_info_new[,1])
df.ticker_info_new[,2] <- as.Date(df.ticker_info_new[,2])
setorder(df.ticker_info_new,cols="startDate")
tail(df.ticker_info_new,20) 

# 与刘江的数据核对
v.dates_new <- v.dates_all_new[!v.dates_all_new%in%v.dates_all]
df.ticker_tmp <- rbind(df.ticker_info, df.ticker_info_new)
df.ticker_info_new_2 <- df.ticker_info
for(s.date in as.character(v.dates_new))
{
  v.tickers_lj <- fread(paste(s.dir.database_handmade,"lj/all_tickers/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  d.date <- as.Date(s.date)
  v.ticker_cur <- rownames(df.ticker_tmp)[df.ticker_tmp$addDate <= d.date]
  if(length(setdiff(v.ticker_cur, v.tickers_lj))|length(setdiff(v.tickers_lj,v.ticker_cur)))
  {
    stop("ticker 不一致")
  }else
  {
    #df.tmp <- data.frame(startDate=rep(s.date,3),addDate=rep(s.date,3),stringsAsFactors=F)
    #rownames(df.tmp) <- setdiff(v.tickers_lj,rownames(df.ticker_info))
    if(length(setdiff(v.tickers_lj,rownames(df.ticker_info_new_2))))
    {
      df.ticker_info_new_2 <- rbind(df.ticker_info_new_2, df.ticker_info_new[setdiff(v.tickers_lj,rownames(df.ticker_info_new_2)),])  
    }
  }
}
# 2020-09-03wind出问题。采用刘江的数据
#{
#  df.tmp <- data.frame(startDate=rep(s.date,3),addDate=rep(s.date,3),stringsAsFactors=F)
#  rownames(df.tmp) <- setdiff(v.tickers_lj,rownames(df.ticker_info))
#  df.ticker_info_new <- rbind(df.ticker_info, df.tmp)  
#}


fwrite(df.ticker_info_new_2, paste(s.dir.database_wind,"ticker_all.csv",sep=""),row.names = T,na="NA")
file.remove(paste(s.dir.data.daily,"ticker_all_copy.csv",sep=""))
file.copy(paste(s.dir.data.daily,"ticker_all.csv",sep=""),paste(s.dir.data.daily,"ticker_all_copy.csv",sep=""))
fwrite(df.ticker_info_new_2, paste(s.dir.data.daily,"ticker_all.csv",sep=""),row.names = T,na="NA")
# ticker_list
s.name <- "all_tickers"
utility.createFolder(paste(s.dir.database_wind,s.name,"/",sep=""))
for(s.date in as.character(v.dates_all_new[!v.dates_all_new%in%v.dates_all]))
{
  print(s.date)
  d.date <- as.Date(s.date)
  v.ticker_cur <- rownames(df.ticker_info_new_2)[df.ticker_info_new_2$addDate <= d.date]
  df <- data.frame(ticker = v.ticker_cur,stringsAsFactors = F)
  fwrite(df, paste(s.dir.database_wind,s.name,"/",s.date,".csv",sep=""), row.names = F,na="NA")
}
# 确认之后 写入正式数据库
s.name <- "all_tickers"
for(s.date in as.character(v.dates_all_new[!v.dates_all_new%in%v.dates_all]))
{
  print(s.date)
  d.date <- as.Date(s.date)
  if(file.exists(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep="")))
  {
    file.remove(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))  
  }
  file.copy(paste(s.dir.database_wind,s.name,"/",s.date,".csv",sep=""), paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
}


# 等到高频的数据和日度的数据更新
# 更新停牌的数据
s.name <- "trading_stopped_stocks_raw"

for(s.date in as.character(v.dates_new))
{
  print(s.date)
  d.date <- as.Date(s.date)
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  v.data_tonglian <- fread(paste(s.dir.database_handmade,"lj/stocks_suspension/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  w_wset_data<-w.wset('tradesuspend',paste('startdate=',s.date,';enddate=',s.date,sep=""))$Data
  print(w_wset_data)
  if(sum(!w_wset_data[,3] %in% v.data_tonglian & w_wset_data[,3] %in% v.ticker_cur))
  {
    stop("wind有我们没有的停牌")
  }
  if(sum(!v.data_tonglian%in%w_wset_data[,3]))
  {
    v.ticker_extra <- v.data_tonglian[!v.data_tonglian%in%w_wset_data[,3]]
    df.volume <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"volume/",s.date,".csv",sep=""))
    v.tmp <- df.volume[v.ticker_extra,1]
    if(s.date!="2021-02-22")
    {
      if(length(which(!is.na(v.tmp)&v.tmp>0)))
      {
        stop("通联多的停牌ticker不能用volume支撑")
      }  
    }else
    {
      print("通联数据有误调整")
      v.data_tonglian <- setdiff(v.data_tonglian,"603650.SH")
    }
  }
  # 停牌数据可以采纳
  i.date <- which(v.dates_all_new==as.Date(s.date))
  df.data_lastDay <- utility.get_stocknames("trading_stopped_stocks",v.dates_all_new[i.date-1])
  rownames(df.data_lastDay) <- df.data_lastDay[,1]
  df.data_lastDay[,1] <- NULL
  v.stoped_tickers <- v.data_tonglian
  df.data <- data.frame(matrix(1,nrow=length(v.stoped_tickers),ncol = 1,dimnames = list(v.stoped_tickers,"stop_days")),stringsAsFactors = F)
  v.tickers_stop_lastDay <- intersect(v.stoped_tickers, rownames(df.data_lastDay))
  df.data[v.tickers_stop_lastDay,1] <- df.data_lastDay[v.tickers_stop_lastDay,1] + 1
  fwrite(df.data, paste(s.dir.database_handmade,"trading_stopped_stocks","/",s.date,".csv",sep=""),row.names = T,na="NA")
  # 写入正式得数据库
  if(file.exists(paste(s.dir.data.stocksnames,"trading_stopped_stocks","/",s.date,".csv",sep="")))
  {
    file.remove(paste(s.dir.data.stocksnames,"trading_stopped_stocks","/",s.date,".csv",sep=""))  
  }
  file.copy(paste(s.dir.database_handmade,"trading_stopped_stocks","/",s.date,".csv",sep=""),paste(s.dir.data.stocksnames,"trading_stopped_stocks","/",s.date,".csv",sep=""))
  print("停牌数据 正式写入")
}

# 检查日度数据
v.names <- c("open","high","low","close","volume","amount")
for(s.date in as.character(v.dates_new))
{
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  for(s.name in v.names)
  {
    df.data_tonglian <- fread(paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)
    if(!identical(df.data_tonglian[,1],v.ticker_cur))
    {
      stop("ticker 不一样")
    }
  }
  # ticker一样以后。和wind比对
  w_wsd_data <- w.wsd(v.ticker_cur,"volume", s.date, s.date,"ShowBlank=NA")$Data
  df.wind_data <- data.frame(data = w_wsd_data[,2],stringsAsFactors = F)
  rownames(df.wind_data) <- w_wsd_data[,1]
  df.data_tonglian <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"volume/",s.date,".csv",sep=""))
  
  v.ticker_stop_wind     <- v.ticker_cur[which(is.na(df.wind_data[,1])|df.wind_data[,1]==0)]
  v.ticker_stop_tonglian <- v.ticker_cur[which(is.na(df.data_tonglian[,1])|df.data_tonglian[,1]==0)]
  if(!identical(v.ticker_stop_tonglian,v.ticker_stop_wind))
  {
    stop()
  }
  if(sum(abs(df.wind_data[v.ticker_cur,1] - df.data_tonglian[v.ticker_cur,1]),na.rm = T)>1e-5)
  {
    stop()
  }
  file.copy(paste(s.dir.database_tonglian,"volume/",s.date,".csv",sep=""), paste(s.dir.data.daily,"volume/",s.date,".csv",sep=""))
  
  v.ticker_normal <- setdiff(v.ticker_cur, v.ticker_stop_wind)
  for(s.name in setdiff(v.names,"volume"))
  {  
    if(s.name == "amount")
    {
      w_wsd_data <- w.wsd(v.ticker_cur,"amt", s.date, s.date,"ShowBlank=NA")$Data
    }else
    {
      w_wsd_data <- w.wsd(v.ticker_cur,s.name, s.date, s.date,"ShowBlank=NA")$Data  
    }
    df.wind_data <- data.frame(data = w_wsd_data[,2],stringsAsFactors = F)
    rownames(df.wind_data) <- w_wsd_data[,1]
    df.data_tonglian <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""))
    
    if(sum(abs(df.wind_data[v.ticker_normal,1] - df.data_tonglian[v.ticker_normal,1])/abs(df.wind_data[v.ticker_normal,1]),na.rm = T)>1e-4)
    {
      stop(s.name)
    }   
  }
  # 以上验证数值
  # 以下验证自身的结构
  df.open   <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
  df.high   <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
  df.low    <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)  
  df.close  <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)  
  df.volume <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)   
  df.amount <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)   
  
  
  df.open[,1] <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"open","/",s.date,".csv",sep=""))
  df.high[,1] <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"high","/",s.date,".csv",sep=""))
  df.low[,1]  <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"low","/",s.date,".csv",sep=""))
  df.close[,1]  <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"close","/",s.date,".csv",sep=""))
  df.volume[,1] <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"volume","/",s.date,".csv",sep=""))
  df.amount[,1] <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,"amount","/",s.date,".csv",sep=""))
  
  v.tmp <- which(df.open[,1]==0 | is.na(df.open[,1]))
  v.tmp <- c(v.tmp,which(df.high[,1]==0 | is.na(df.high[,1])))
  v.tmp <- c(v.tmp,which(df.low[,1]==0 | is.na(df.low[,1])))
  v.tmp <- c(v.tmp,which(df.close[,1]==0 | is.na(df.close[,1])))
  v.tmp <- unique(v.tmp)
  d.len <- length(which(df.volume[v.tmp,1]!=0 & !is.na(df.volume[v.tmp,1])))
  d.len <- d.len + length(which(df.amount[v.tmp,1]!=0 & !is.na(df.amount[v.tmp,1])))
  if(d.len)
  {
    stop(s.date)
  }
  v.tickers_stop <- rownames(df.open)[which(df.open[,1]==0)]
  if(sum(!v.tickers_stop%in%utility.get_stocknames("trading_stopped_stocks",s.date)[,1]))
  {
    stop(s.date)
  }
  fwrite(df.open  , paste(s.dir.data.daily,"open/",s.date,".csv",sep=""),row.names = T,na="NA")
  fwrite(df.high  , paste(s.dir.data.daily,"high/",s.date,".csv",sep=""),row.names = T,na="NA")
  fwrite(df.low   , paste(s.dir.data.daily,"low/",s.date,".csv",sep=""),row.names = T,na="NA")
  fwrite(df.close , paste(s.dir.data.daily,"close/",s.date,".csv",sep=""),row.names = T,na="NA")
  fwrite(df.volume, paste(s.dir.data.daily,"volume/",s.date,".csv",sep=""),row.names = T,na="NA")
  fwrite(df.amount, paste(s.dir.data.daily,"amount/",s.date,".csv",sep=""),row.names = T,na="NA")
  print("日度数据 正式写入数据库")
}

# 转换高频数据
s.dir_origMinuteDate<- "I:/data/minutes/stocks/"
v.data_classes <- c("open","high","low","close","volume","amount","vwap")
v.data_classes_inOrigData <- c("openPrice","highPrice","lowPrice","closePrice","totalVolume","totalValue","vwap")

s.year <- "2021"
{
  s.dir_minute_year <- paste(s.dir_origMinuteDate,s.year,"/",sep="")
  v.files <- list.files(s.dir_minute_year)
  v.dates_all_files <-   as.Date(as.vector(sapply(v.files, function(x){as.Date(substring(x,1,8), format = "%Y%m%d")})),origin = as.Date("1970-01-01"))
  v.dates_left <- v.dates_all_files[!v.dates_all_files %in% as.Date(substring(list.files(paste(s.dir.data.minute,"vwap/",sep="")),1,10))]
  v.dates_left <- v.dates_left[v.dates_left %in% v.dates_all_new]
  if(length(v.dates_left)>0)
  {
  for(i.file in seq(1,length(v.dates_left),1))
  {
    d.date<- v.dates_left[i.file]
    s.date <- as.character(d.date)
    s.file <- paste(s.dir_minute_year, str_remove_all(d.date,"-"), ".csv",sep="")
    print(d.date)
    df.data <- fread(s.file,header = T, stringsAsFactors = F, data.table = F)
    s.file_plus <- paste(s.dir.database_tonglian,"data_plus/",d.date,".csv",sep="")
    if(file.exists(s.file_plus))
    {
      print("data plus")
      df.data_plus <- fread(s.file_plus, header = T,stringsAsFactors = F,data.table = F)
      df.data <- rbind(df.data, df.data_plus)
    }
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    print(length(v.ticker_cur))
    ## output this file in the order of v.tickers_all
    l.cur_data <- vector("list",length(v.data_classes))
    names(l.cur_data) <- v.data_classes
    for(s.data_class in v.data_classes)
    {
      l.cur_data[[s.data_class]] <- as.data.frame(matrix(NA, nrow = length(v.ticker_cur), ncol= length(v.minutes_DataFormat)),stringsAsFactors = F)
      rownames(l.cur_data[[s.data_class]]) <- v.ticker_cur
      colnames(l.cur_data[[s.data_class]]) <- v.minutes_DataFormat
    }
    v.tickers_inThisFile <- sapply(unique(df.data$ticker),function(x){getFullTicker(x)})
    if(!sum(v.tickers_inThisFile%in%v.ticker_cur))
    {
      print("extra tickers")
    }
    
    for(i.ticker in 1:length(v.tickers_inThisFile))
    {
      s.ticker <- v.tickers_inThisFile[i.ticker]
      if(i.ticker %% 500 == 0)
        print(i.ticker)
      s.ticker_orig <- as.character(as.integer(substring(s.ticker,1,6)))
      df.data_thisTicker <- df.data[(1+(i.ticker-1)*241):(i.ticker*241),]
      for(i.data_class_inOrig in 1:length(v.data_classes_inOrigData))
      {
        s.data_class_inOrig <- v.data_classes_inOrigData[i.data_class_inOrig]
        l.cur_data[[v.data_classes[i.data_class_inOrig]]][s.ticker,] <- as.numeric(df.data_thisTicker[-1,s.data_class_inOrig])
      }
    }
    # 统一处置na 
    df.volume <- utility.load_data_withoutDate(paste(s.dir.data.daily,"volume/",d.date,".csv",sep=""))
    v.tickers_NA <- v.ticker_cur[is.na(df.volume[,1])|df.volume[,1]==0]
    for(s.data_class in v.data_classes)
    {
      l.cur_data[[s.data_class]][v.tickers_NA,] <- NA
      if(sum(!v.ticker_cur[which(is.na(l.cur_data[[s.data_class]][,1]))] %in% v.tickers_NA))
      {
        stop("有高频ticker不是NA,但是日度是NA")
      }
      if(sum(!v.tickers_NA%in%v.ticker_cur[which(is.na(l.cur_data[[s.data_class]][,1]))]))
      {
        stop("有日度ticker不是NA,但是高频是NA")
      }
      # 以上两个是为了相互对照
      if(!identical(rownames(l.cur_data[[s.data_class]]),v.ticker_cur))
      {
        stop()
      }
      fwrite(l.cur_data[[s.data_class]], paste(s.dir.data.minute,s.data_class,"/",d.date,".csv",sep=""),row.names = T,na="NA")
    } 
    print("分钟数据 正式写入数据库")
  }  
  }
  
}

# 更新分红数据
for(s.date in as.character(v.dates_new))
{
df.adj_data <- fread(paste(s.dir.database_tonglian,"fenhong/stocks_back_adj_factor_",str_remove_all(s.date,"-"),".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)
df.adj_data$id <- NULL
s.name <- "分红配股数据_nameChanged"


  df.cur_adj <- df.adj_data[as.Date(df.adj_data$div_date) == as.Date(s.date),]
  if(dim(df.cur_adj)[1] >0)
  {
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    df.cur_adj <- df.cur_adj[df.cur_adj$ticker%in%v.ticker_cur,]
    df.cur_new <- df.cur_adj[,c("ticker","div_date",
                                "per_cash_div","per_share_div_ratio",
                                "per_share_trans_ratio",
                                "allotment_ratio","allotment_price")]      
    rownames(df.cur_new) <- df.cur_new$ticker
    df.cur_new$ticker <- NULL
    df.cur_new$div_date <- NULL
    df.cur_new[df.cur_new=="NULL"] <-NA
    m.tmp <- matrix(apply(df.cur_new,2,function(x){as.numeric(x)}),nrow=dim(df.cur_new)[1])
    rownames(m.tmp) <- rownames(df.cur_new)
    colnames(m.tmp) <- colnames(df.cur_new)
    df.tmp <- as.data.frame(m.tmp,stringsAsFactors = F)
    fwrite(df.tmp,paste(s.dir.database_tonglian,"stock_names/",s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }else
  {
    df <- data.frame(matrix(NA,nrow=0,ncol=5),stringsAsFactors = F)
    colnames(df) <- c("per_cash_div","per_share_div_ratio",
                      "per_share_trans_ratio",
                      "allotment_ratio","allotment_price")
    fwrite(df,paste(s.dir.database_tonglian,"stock_names/",s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }

}
# 手动填入wind的分红数据
s.dir.fenhong_wind <- paste(s.dir.database_wind,"分红数据/",sep="")
s.dir.fenhong_tonglian <- paste(s.dir.database_tonglian,"stock_names/分红配股数据_nameChanged/",sep="")
for(s.date in as.character(v.dates_all_new[!v.dates_all_new%in%v.dates_all]))
{
  d.date <- as.Date(s.date)
  df.fhsp_cur <- data.frame(matrix(NA,nrow=0,ncol=5),stringsAsFactors = F)
  colnames(df.fhsp_cur) <- c("per_cash_div","per_share_div_ratio","per_share_trans_ratio","allotment_ratio","allotment_price")
  
  # 分红配息
  w_wset_data<-w.wset('bonus',paste('orderby=股权登记日;startdate=',d.date-15,';enddate=',d.date,';sectorid=a001010100000000',sep=""))
  w_data <- w_wset_data$Data
  w_data_today <- w_data[which(w_data$progress == "实施完毕" & as.Date(w_data$exrights_exdividend_date,origin = as.Date("1899-12-30")) == d.date&w_data$sec_type=="A股"),]
  w_data_today <- w_data_today[substring(w_data_today$wind_code,1,3) != "688",]
  if(dim(w_data_today)[1])
  {
    df.fhsp_cur[1:dim(w_data_today)[1],1:3] <- w_data_today[,c("dividendsper_share_pretax","sharedividends_proportion","shareincrease_proportion")]
    rownames(df.fhsp_cur) <- w_data_today$wind_code  
  }
  # 配股
  w_wset_data<-w.wset('rightsissueimplementation',paste('startdate=',d.date-15,';enddate=',d.date,';sectorid=a001010100000000;windcode=',sep=""))
  w_data <- w_wset_data$Data
  w_data_today <- w_data[as.Date(w_data$ex_rights_date,origin = as.Date("1899-12-30")) == as.Date(s.date) & substring(w_data$wind_code,1,3)!="688",]
  if(dim(w_data_today)[1])
  {
    for(i.row in 1:dim(w_data_today)[1])
    {
      df.fhsp_cur[w_data_today$wind_code[i.row],"allotment_ratio"] <- w_data_today$rights_issue_ratio[i.row]
      df.fhsp_cur[w_data_today$wind_code[i.row],"allotment_price"] <- w_data_today$rights_issue_price[i.row]
    }
  }
  
  df.fhsp_cur[is.na(df.fhsp_cur)] <- 0
  fwrite(df.fhsp_cur, paste(s.dir.fenhong_wind,s.date,".csv",sep=""),row.names = T,na="NA")
}


for(s.date in as.character(v.dates_all_new[!v.dates_all_new%in%v.dates_all]))
{
  if(s.date != "2020-07-28" & s.date!="2020-08-13" & s.date !="2020-09-17" &s.date !="2020-09-29"&s.date !="2020-12-08")
  {
    df.fenhong_wind <- utility.load_data_withoutDate(paste(s.dir.fenhong_wind,s.date,".csv",sep=""))
    df.fenhong_tonglian <- utility.load_data_withoutDate(paste(s.dir.fenhong_tonglian,s.date,".csv",sep=""))
    # 先验证标的是否一样
    v.fenhong_ticker_wind <- rownames(df.fenhong_wind)
    v.fenhong_ticker_tonglian <- rownames(df.fenhong_tonglian)
    if(length(setdiff(v.fenhong_ticker_tonglian,v.fenhong_ticker_wind)))
    {
      stop(s.date)
    }
    if(length(setdiff(v.fenhong_ticker_wind,v.fenhong_ticker_tonglian)))
    {
      stop(s.date)
    }
    
    if(length(v.fenhong_ticker_tonglian))
    {
    df.fenhong_wind[is.na(df.fenhong_wind)] <- 0
    df.fenhong_tonglian[is.na(df.fenhong_tonglian)] <- 0
    if(sum(df.fenhong_tonglian  - df.fenhong_wind)>1e-3)
    {
      stop(s.date)
    }  
    } 
  }
  print("分红数据与wind核对一致")
}
# 正式写入数据库
s.name <- "分红配股数据"
for(s.date in as.character(v.dates_new))
{
  df.fenhong_tonglian <- utility.load_data_withoutDate(paste(s.dir.fenhong_tonglian,s.date,".csv",sep=""))
  if(file.exists(paste(s.dir.database_handmade,"经与wind比对需修正的复权因子调整数据/",s.date,".csv",sep="")))
  {
    print(s.date)
    df.plus <- utility.load_data_withoutDate(paste(s.dir.database_handmade,"经与wind比对需修正的复权因子调整数据/",s.date,".csv",sep=""))
    if(dim(df.plus)[1]==1)
    {
      if(length(which(rownames(df.fenhong_tonglian) == rownames(df.plus))))
      {
        df.fenhong_tonglian[which(rownames(df.fenhong_tonglian) == rownames(df.plus)),] <- df.plus  
      }else
      {
        df.fenhong_tonglian[dim(df.fenhong_tonglian)[1]+1,] <-  df.plus 
      }
      print("分红数据 正式写入数据库")
      fwrite(df.fenhong_tonglian, paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")    
    }else
    {
      fwrite(df.plus, paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")      
    }
  }else
  {
    print("分红数据 正式写入数据库")
    fwrite(df.fenhong_tonglian, paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")    
  }
}


# 手动填入wind的分红数据
s.dir.fenhong_wind <- paste(s.dir.database_wind,"分红数据_按股权登记日划分/",sep="")
utility.createFolder(s.dir.fenhong_wind)
v.dates_tmp <- as.character(v.dates_new)
for(s.date in v.dates_tmp)
{
  d.date <- as.Date(s.date)
  df.fhsp_cur <- data.frame(matrix(NA,nrow=0,ncol=5),stringsAsFactors = F)
  colnames(df.fhsp_cur) <- c("per_cash_div","per_share_div_ratio","per_share_trans_ratio","allotment_ratio","allotment_price")
  
  # 分红配息
  w_wset_data<-w.wset('bonus',paste('orderby=股权登记日;startdate=',d.date,';enddate=',d.date,';sectorid=a001010100000000',sep=""))
  w_data <- w_wset_data$Data
  if(d.date == today())
  {
    w_data_today <- w_data[w_data$progress=="等待实施(含股权登记当天)" & as.Date(w_data$shareregister_date,origin = as.Date("1899-12-30")) == d.date&w_data$sec_type=="A股",]  
  }else
  {
    w_data_today <- w_data[w_data$progress=="实施完毕" & as.Date(w_data$shareregister_date,origin = as.Date("1899-12-30")) == d.date&w_data$sec_type=="A股",]  
  }
  
  w_data_today <- w_data_today[substring(w_data_today$wind_code,1,3) != "688",]
  if(dim(w_data_today)[1])
  {
    df.fhsp_cur[1:dim(w_data_today)[1],1:3] <- w_data_today[,c("dividendsper_share_pretax","sharedividends_proportion","shareincrease_proportion")]
    rownames(df.fhsp_cur) <- w_data_today$wind_code  
  }
  # 配股
  w_wset_data<-w.wset('rightsissueimplementation',paste('startdate=',d.date-15,';enddate=',d.date,';sectorid=a001010100000000;windcode=',sep=""))
  w_data <- w_wset_data$Data
  w_data_today <- w_data[as.Date(w_data$record_date,origin = as.Date("1899-12-30")) == as.Date(s.date) & substring(w_data$wind_code,1,3)!="688",]
  if(dim(w_data_today)[1])
  {
    for(i.row in 1:dim(w_data_today)[1])
    {
      df.fhsp_cur[w_data_today$wind_code[i.row],"allotment_ratio"] <- w_data_today$rights_issue_ratio[i.row]
      df.fhsp_cur[w_data_today$wind_code[i.row],"allotment_price"] <- w_data_today$rights_issue_price[i.row]
    }
  }
  
  df.fhsp_cur[is.na(df.fhsp_cur)] <- 0
  fwrite(df.fhsp_cur, paste(s.dir.fenhong_wind,s.date,".csv",sep=""),row.names = T,na="NA")
}


s.dir.fenhong_tonglian <- paste(s.dir.database_tonglian,"div_allot/",sep="")
for(s.date in v.dates_tmp)
{
  if(s.date != "2020-07-28" & s.date!="2020-08-13" & s.date !="2020-09-17"& s.date !="2020-12-08")
  {
    df.fenhong_wind <- utility.load_data_withoutDate(paste(s.dir.fenhong_wind,s.date,".csv",sep=""))
    df.fenhong_tonglian <- utility.load_data_withoutDate(paste(s.dir.fenhong_tonglian,s.date,".csv",sep=""))
    v.fenhong_ticker_wind <- rownames(df.fenhong_wind)
    v.fenhong_ticker_tonglian <- rownames(df.fenhong_tonglian)
    if(length(setdiff(v.fenhong_ticker_tonglian,v.fenhong_ticker_wind)))
    {
      stop(s.date)
    }
    if(length(setdiff(v.fenhong_ticker_wind,v.fenhong_ticker_tonglian)))
    {
      stop(s.date)
    }
    if(length(v.fenhong_ticker_tonglian))
    {
      df.fenhong_wind[is.na(df.fenhong_wind)] <- 0
      df.fenhong_tonglian[is.na(df.fenhong_tonglian)] <- 0
      if(sum(df.fenhong_tonglian  - df.fenhong_wind)>1e-3)
      {
        stop(s.date)
      }  
    } 
  }
  print("分红数据与wind核对一致")
}

# 正式写入数据库
s.name <- "分红配股数据_按股权登记日划分"
utility.createFolder(paste(s.dir.data.stocksnames,s.name,"/",sep=""))
for(s.date in v.dates_tmp)
{
  df.fenhong_tonglian <- utility.load_data_withoutDate(paste(s.dir.fenhong_tonglian,s.date,".csv",sep=""))
  if(file.exists(paste(s.dir.database_handmade,"经与wind比对需修正的复权因子调整数据_股权登记/",s.date,".csv",sep="")))
  {
    if(file.exists(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep="")))
    {
      file.remove(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
    }
    file.copy(paste(s.dir.database_handmade,"经与wind比对需修正的复权因子调整数据_股权登记/",s.date,".csv",sep=""),
              paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
  }else
  {
    print("分红数据 正式写入数据库")
    fwrite(df.fenhong_tonglian, paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")  
  }
}


# 股本变动数据
options("scipen"=100)

df.shares_change_tonglian <- fread(paste(s.dir.database_tonglian,"guben/stocks_shares_change_",str_remove_all(tail(v.dates_new,1),"-"),".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)
v.ticker_all_new <- rownames(df.ticker_info_new_2)
print(length(v.ticker_all_new))

df.share_float_tonglian <- data.frame(matrix(NA, nrow = length(v.dates_new), ncol=length(v.ticker_all_new),dimnames = list(as.character(v.dates_new),v.ticker_all_new)),stringsAsFactors = F)
df.share_total_tonglian <- data.frame(matrix(NA, nrow = length(v.dates_new), ncol=length(v.ticker_all_new),dimnames = list(as.character(v.dates_new),v.ticker_all_new)),stringsAsFactors = F)
colnames(df.share_float_tonglian) <- v.ticker_all_new
colnames(df.share_total_tonglian) <- v.ticker_all_new

for(i.ticker in 1:length(v.ticker_all_new))
{
  s.ticker <- v.ticker_all_new[i.ticker]
  df.shareChange_thisTicker <- df.shares_change_tonglian[df.shares_change_tonglian$ticker==s.ticker,]
  df.shareChange_thisTicker <- df.shareChange_thisTicker[df.shareChange_thisTicker$publish_date!="NULL",]
  setorder(df.shareChange_thisTicker,cols="change_date")
  if(dim(df.shareChange_thisTicker)[1]>1)
  {
    for(i in 2:dim(df.shareChange_thisTicker)[1])
    {
      if(df.shareChange_thisTicker[i,"total_shares"]==df.shareChange_thisTicker[i-1,"total_shares"] & df.shareChange_thisTicker[i,"float_a"]==df.shareChange_thisTicker[i-1,"float_a"])
      {
        df.shareChange_thisTicker[i,"total_shares"] <- 0 
      }
    }
    df.shareChange_thisTicker <- df.shareChange_thisTicker[df.shareChange_thisTicker$total_shares>0&!is.na(df.shareChange_thisTicker$total_shares),]
  }
  
  # 在add_Date的后一天纳入
  i.start <- which(v.dates_new > min(as.Date(df.shareChange_thisTicker$publish_date)))[1]
  for(i.date in i.start:length(v.dates_new))
  {
    s.date <- as.character(v.dates_new[i.date])
    d.date <- as.Date(s.date)
    v.locs <- which(as.Date(df.shareChange_thisTicker$change_date) <= d.date)
    if(length(v.locs)==0)
    {
      print(s.date)
    }else
    {
      j <- max(v.locs)
      while (as.Date(df.shareChange_thisTicker$publish_date[j])>d.date)
      {
        j <- j - 1
      }
      df.share_float_tonglian[s.date, s.ticker] <- as.numeric(df.shareChange_thisTicker[j,"float_a"])
      df.share_total_tonglian[s.date, s.ticker] <- as.numeric(df.shareChange_thisTicker[j,"total_shares"])  
      
    }
  }
}

s.name <- "share_float"
utility.createFolder(paste(s.dir.database_tonglian,s.name,"/",sep=""))
for(s.date in as.character(v.dates_new))
{
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  df.data_cur <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
  rownames(df.data_cur) <- v.ticker_cur
  colnames(df.data_cur) <- "data"
  df.data_cur[v.ticker_cur,1] <- as.numeric(df.share_float_tonglian[s.date,v.ticker_cur])
  fwrite(df.data_cur, paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")  
}

# 与wind核对
for(s.date in as.character(v.dates_new))
{
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  df.data_tonglian <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""))
  df.wind_data <- w.wsd(v.ticker_cur,"mkt_cap_ashare", s.date, s.date,"ShowBlank=NA")$Data
  rownames(df.wind_data) <- df.wind_data[,1]
  df.wind_data[,1] <- NULL
  df.close  <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close/",s.date,".csv",sep=""))
  v.wind_share_float <- as.numeric(df.wind_data[,1]/df.close[,1])
  print(v.ticker_cur[which(abs(v.wind_share_float - df.data_tonglian[v.ticker_cur,1])/abs(v.wind_share_float)>1)])
}
for(s.date in as.character(v.dates_new))
{
  file.copy(paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""), paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""))
}

s.name <- "share_total"
for(s.date in as.character(v.dates_new))
{
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  df.data_cur <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
  rownames(df.data_cur) <- v.ticker_cur
  colnames(df.data_cur) <- "data"
  df.data_cur[v.ticker_cur,1] <- as.numeric(df.share_total_tonglian[s.date,v.ticker_cur])
  fwrite(df.data_cur, paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")  
}
print("分红和股本数据更新完成")

# ST
s.name <- "ST"
for(s.date in as.character(v.dates_new))
{
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  v.ST_handmade <-  fread(paste(s.dir.database_handmade,"lj/st/",s.date,".csv",sep=""),stringsAsFactors = F,data.table = F,header = T)[,1]
  v.wind_names <- w.wsd(v.ST_handmade,"sec_name", s.date, s.date,"ShowBlank=NA")$Data[,"SEC_NAME"]
  if(sum(!grepl("S",v.wind_names) & !grepl("退",v.wind_names)))
  {
    if(s.date=="2021-03-12") # 这一天有一个“江泉实业”实际上已经是ST了。但wind的名字有问题
    {
      
    }else if(s.date == "2021-03-15")# 这一天有一个“鑫科材料”撤消ST
    {
      
    }else
    {
      #stop()
      # 在2021年3月19日干脆不再做这个比较了。wind有更新慢的问题，通联也有更新慢的问题。差别不会太大。
    }
  }
  file.copy(paste(s.dir.database_handmade,"lj/st/",s.date,".csv",sep=""), paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
  print("st数据 正式写入数据库")
}

# 自动取徐隽泉做的数据
for(s.date in as.character(v.dates_new))
{
  s.dir_tmp <- paste("D:/handmade_data/lj/industry/",sep="")
  for(s.field in v.field_sector)
  {
    s.dir <- paste(s.dir_tmp, s.field,"/",sep="")
    print("copy industry datas")
    file.remove(paste(s.dir.database_tonglian,"stock_names/",s.field,"/",s.date,".csv",sep=""))
    file.copy(paste(s.dir,s.date,".csv",sep=""), 
              paste(s.dir.database_tonglian,"stock_names/",s.field,"/",s.date,".csv",sep=""))
  }  
}

for(s.field in v.field_sector)
{
  print(s.field)
  for(s.date in as.character(v.dates_new))
  {
    v.data_tongian <- fread(paste(s.dir.database_tonglian,"stock_names/",s.field,"/",s.date,".csv",sep=""),stringsAsFactors = F,data.table = F,header = T)[,1]
    v.data_wind <- w.wsd(v.data_tongian,"industry_citic", s.date, s.date,"industryType=1;ShowBlank=NA")$Data[,"INDUSTRY_CITIC"]
    if(any(v.data_wind!="NaN"&v.data_wind!=s.field))
    {
      stop()
    }
  }
}
for(s.field in v.field_sector)
{
  for(s.date in as.character(v.dates_new))
  {
    file.remove(paste(s.dir.data.stocksnames,s.field,"/",s.date,".csv",sep=""))
    file.copy(paste(s.dir.database_tonglian,"stock_names/",s.field,"/",s.date,".csv",sep=""),paste(s.dir.data.stocksnames,s.field,"/",s.date,".csv",sep=""))
  }
}
print("行业数据 正式写入数据库")


# 退市预警
s.name <- "delist_warning"
for(s.date in as.character(v.dates_new))
{
  if(file.exists(paste(s.dir.database_handmade,s.name,"/",s.date,".csv",sep="")))
  {
    file.copy(paste(s.dir.database_handmade,s.name,"/",s.date,".csv",sep=""),paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))  
  }else
  {
    if(s.date == as.character(v.dates_new)[1])
    {
      s.lastDate <- as.character(tail(v.dates_all,1))
    }else
    {
      s.lastDate <- as.character(v.dates_all_new[which(v.dates_all_new==as.Date(s.date))[1]-1])
    }
    if(file.exists(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep="")))
    {
      file.remove(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))  
    }
    file.copy(paste(s.dir.data.stocksnames,s.name,"/",s.lastDate,".csv",sep=""), paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
  }
  
}

print("退市预警 正式写入数据库")




# 后复权因子
for(i.date in 1:length(v.dates_new))
{
  s.date <- v.dates_new[i.date]
  print(s.date)
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  df.exDivDate <- utility.get_stocknames("分红配股数据",s.date)
  
  if(i.date == 1)
  {
    d.lastTradeDate <- v.dates_all[tail(which(v.dates_all<as.Date(s.date)),1)]
  }else
  {
    d.lastTradeDate <- v.dates_new[i.date-1]
  }
  df.adj_factors_lastDate <- utility.load_data_withoutDate(paste(s.dir.data.daily,"adj_factors/",d.lastTradeDate,".csv",sep=""))
  df.adj_factors_new <- data.frame(matrix(1,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
  df.adj_factors_new[rownames(df.adj_factors_lastDate),1] <- as.numeric(df.adj_factors_lastDate[,1])
  if(dim(df.exDivDate)[1]>0)
  {
    df.exDivDate[is.na(df.exDivDate)] <- 0
    v.ticker_exdivDate <- df.exDivDate[,1]  
    
    # 获得前一天的不复权价格
    v.Pt_1 <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close/",d.lastTradeDate,".csv",sep=""))[v.ticker_exdivDate,1]
    if(length(which(is.na(v.Pt_1))))
    {
      stop("1")
    }
    # 获得前收盘价
    v.Pext <- (v.Pt_1 - df.exDivDate[,"per_cash_div"]+df.exDivDate[,"allotment_price"]*df.exDivDate[,"allotment_ratio"])/
      (1+df.exDivDate[,"per_share_div_ratio"]
       +df.exDivDate[,"per_share_trans_ratio"]
       +df.exDivDate[,"allotment_ratio"])
    v.Pext <- as.numeric(sapply(v.Pext,function(x){utility.myRound(x,2)}))
    # 最新福泉因子
    v.adjFactor_newest <- v.Pt_1/v.Pext
    df.adj_factors_new[v.ticker_exdivDate,1] <- df.adj_factors_new[v.ticker_exdivDate,1] * v.adjFactor_newest  
  }
  fwrite(df.adj_factors_new,paste(s.dir.data.daily,"adj_factors/",s.date,".csv",sep=""),row.names = T,na="NA")
  print("复权因子 正式写入数据库")
}
s.name <- "adj_factors_tomorow"
utility.createFolder(paste(s.dir.data.daily,s.name,"/",sep=""))
for(s.date in setdiff(as.character(v.dates_all_new),substring(list.files(paste(s.dir.data.daily,s.name,"/",sep="")),1,10)))
{
  print(s.date)
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  if(which(v.dates_all_new==as.Date(s.date))[1] < length(v.dates_all_new))
  {
    # 不应该算作未来数据。因为明天的后复权因子都是用今天的数据算出来的。
    print(paste(s.date,"复制而来"))
    df <- utility.load_data_withoutDate(paste(s.dir.data.daily,"adj_factors/",v.dates_all_new[which(v.dates_all_new==as.Date(s.date))[1]+1],".csv",sep=""))
    
    df_new <- data.frame(as.numeric(df[v.ticker_cur,]),stringsAsFactors = F)
    rownames(df_new) <- v.ticker_cur
    colnames(df_new) <- c("data")
    
    fwrite(df_new, paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }else
  {
    df.exDivDate <- utility.get_stocknames("分红配股数据_按股权登记日划分",s.date)
    df.adj_factors_lastDate <- utility.load_data_withoutDate(paste(s.dir.data.daily,"adj_factors/",s.date,".csv",sep=""))
    df.adj_factors_new <- data.frame(matrix(1,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
    df.adj_factors_new[rownames(df.adj_factors_lastDate),1] <- as.numeric(df.adj_factors_lastDate[,1])
    if(dim(df.exDivDate)[1]>0)
    {
      df.exDivDate[is.na(df.exDivDate)] <- 0
      v.ticker_exdivDate <- df.exDivDate[,1]  
      
      # 获得前一天的不复权价格
      v.Pt_1 <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close/",s.date,".csv",sep=""))[v.ticker_exdivDate,1]
      if(length(which(is.na(v.Pt_1))))
      {
        stop("1")
      }
      # 获得前收盘价
      v.Pext <- (v.Pt_1 - df.exDivDate[,"per_cash_div"]+df.exDivDate[,"allotment_price"]*df.exDivDate[,"allotment_ratio"])/
        (1+df.exDivDate[,"per_share_div_ratio"]
         +df.exDivDate[,"per_share_trans_ratio"]
         +df.exDivDate[,"allotment_ratio"])
      v.Pext <- as.numeric(sapply(v.Pext,function(x){utility.myRound(x,2)}))
      # 最新福泉因子
      v.adjFactor_newest <- v.Pt_1/v.Pext
      df.adj_factors_new[v.ticker_exdivDate,1] <- df.adj_factors_new[v.ticker_exdivDate,1] * v.adjFactor_newest  
    }
    fwrite(df.adj_factors_new,paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
    print("明天的复权因子 正式写入数据库")  
  }
}
# vwap
options(warn=2)
s.name <- "vwap"
for(s.date in as.character(v.dates_new))
{
  df.amount <- utility.load_data_withoutDate(paste(s.dir.data.daily,"amount/",s.date,".csv",sep=""))
  df.volume <- utility.load_data_withoutDate(paste(s.dir.data.daily,"volume/",s.date,".csv",sep=""))
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  df.vwap <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
  df.vwap[v.ticker_cur,1] <- as.numeric(df.amount[,1]) / as.numeric(df.volume[,1])
  if(length(which(is.na(df.vwap[,1]&!df.volume[,1]==0&!is.na(df.volume[,1])))))
  {
    stop(s.date)
  }
  
  df.vwap[is.na(df.vwap[,1]),1] <- 0
  
  df.close <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close/",s.date,".csv",sep=""))
  df.vwap[which(is.na(df.close[,1])),1] <- NA
  
  
  print("vwap数据 正式写入数据库")  
  fwrite(df.vwap, paste(s.dir.data.daily,"vwap/",s.date,".csv",sep=""),row.names = T,na="NA")  
}
{
  # 流通市镇
  s.name <- "mkt_cap_float"
  utility.createFolder(paste(s.dir.data.daily,s.name,"/",sep=""))
  for(s.date in as.character(v.dates_new))
  {
    df.close      <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close/",s.date,".csv",sep=""))
    df.share_float<- utility.load_data_withoutDate(paste(s.dir.data.daily,"share_float/",s.date,".csv",sep="")) 
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    df.mkt_cap_float <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
    df.mkt_cap_float[v.ticker_cur,1] <- as.numeric(df.share_float[,1]) * df.close[,1]
    if(length(which(is.na(df.mkt_cap_float[,1])&!is.na(df.close[,1]))))
    {
      print(s.date)
    }
    fwrite(df.mkt_cap_float, paste(s.dir.data.daily,"mkt_cap_float/",s.date,".csv",sep=""),row.names = T,na="NA")  
  }
  
  # 后复权数据
  v.names <- c("open","high","low","close","vwap")
  v.names_b <- paste(v.names,"_b",sep="")
  for(i.name in 1:length(v.names))
  {
    s.name   <- v.names[i.name]
    s.name_b <- v.names_b[i.name]
    for(s.date in as.character(v.dates_new))
    {
      df.data_orig <- utility.load_data_withoutDate(paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""))
      df.adj_factors <- utility.load_data_withoutDate(paste(s.dir.data.daily,"adj_factors","/",s.date,".csv",sep=""))
      
      v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
      df.data_b    <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
      
      df.data_b[,1] <- df.adj_factors[,1] * df.data_orig[,1]
      fwrite(df.data_b, paste(s.dir.data.daily,s.name_b,"/",s.date,".csv",sep=""),row.names = T,na="NA")
    }
  }
  
  
  
  # 总市值
  s.name <- "mkt_cap_total2"
  for(s.date in as.character(v.dates_new))
  {
    s.file <- paste(s.dir.database_wind,s.name,"/",s.date,".csv",sep="")
    if(!file.exists(s.file))
    {
      print(s.date)
      v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
      w_wsd_data<-w.wsd(v.ticker_cur,"mkt_cap_ard", s.date, s.date, "ShowBlank=NA;PriceAdj=B")$Data[,2]
      df.data <- data.frame(data = w_wsd_data,stringsAsFactors = F)
      rownames(df.data) <- v.ticker_cur
      fwrite(df.data, s.file, row.names = T,na="NA")  
    }  
  }
  # 用通联的数据补齐NA
  s.name <- "mkt_cap_total2"
  for(s.date in as.character(v.dates_new))
  {
    df.wind_data_totalMkt     <- utility.load_data_withoutDate(paste(s.dir.database_wind,s.name,"/",s.date,".csv",sep=""))
    df.tonglian_close         <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close","/",s.date,".csv",sep=""))
    df.tonglian_share_total   <- utility.load_data_withoutDate(paste(s.dir.data.daily,"share_total","/",s.date,".csv",sep=""))
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    df.data <-  data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
    df.data[v.ticker_cur,1] <- df.wind_data_totalMkt[v.ticker_cur,1]
    
    v.ticker_needTonglian <- v.ticker_cur[which(is.na(df.data[,1])|df.data[,1]==0)]
    df.data[v.ticker_needTonglian,1] <- df.tonglian_close[v.ticker_needTonglian,1] * as.numeric(df.tonglian_share_total[v.ticker_needTonglian,1])
    
    if(length(which(is.na(df.data[,1])&!is.na(df.tonglian_close[,1]))))
    {
      stop(s.date)
    }
    fwrite(df.data,paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }
  #  换手率
  s.name <- "turn"
  utility.createFolder(paste(s.dir.data.daily,s.name,"/",sep=""))
  for(s.date in as.character(v.dates_new))
  {
    
    df.tonglian_volume        <- utility.load_data_withoutDate(paste(s.dir.data.daily,"volume","/",s.date,".csv",sep=""))
    df.tonglian_share_float   <- utility.load_data_withoutDate(paste(s.dir.data.daily,"share_float","/",s.date,".csv",sep=""))
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    df.data <-  data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
    df.data[v.ticker_cur,1] <- df.tonglian_volume[,1] / as.numeric(df.tonglian_share_float[,1])
    
    
    fwrite(df.data,paste(s.dir.data.daily,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }
  v.dirs <- list.dirs(s.dir.data.daily,recursive = F)
  for(s.dir in paste(v.dirs,"/",sep=""))
  {
    print(max(as.Date(substring(list.files(s.dir),1,10))))
  }
  
  s.name <- "pct_chg"
  for(i.date in 1:length(v.dates_new))
  {
    s.date <- as.character(v.dates_new[i.date])
    print(s.date)
    if(i.date == 1)
    {
      d.lastTradeDate <- v.dates_all[tail(which(v.dates_all<as.Date(s.date)),1)]
    }else
    {
      d.lastTradeDate <- v.dates_new[i.date-1]
    }
    v.ticker_last <- utility.getAllAvailableTicker_new(as.character(d.lastTradeDate))
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    df.close_last <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close_b/",d.lastTradeDate,".csv",sep=""))
    df.close      <- utility.load_data_withoutDate(paste(s.dir.data.daily,"close_b/",s.date,".csv",sep=""))
    
    df <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol=1,dimnames = list(v.ticker_cur,"data")),stringsAsFactors = F)
    df[v.ticker_last,1] <- as.numeric(df.close[v.ticker_last,1] / df.close_last[v.ticker_last,1] - 1)
    fwrite(df, paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }
  
  s.name <- "pct_chg"
  for(i.date in 1:length(v.dates_new))
  {
    s.date <- as.character(v.dates_new[i.date])
    print(s.date)
    df.pctchg_tonglian <- utility.load_data_withoutDate(paste(s.dir.database_tonglian,s.name,"/",s.date,".csv",sep=""))
    v.tickers_NA <- rownames(df.pctchg_tonglian)[which(is.na(df.pctchg_tonglian[,1]))]
    print(v.tickers_NA)
    w_wsd_data<-w.wsd(v.tickers_NA,"pct_chg",s.date,s.date)$Data
    
    df.pctchg_wind <- df.pctchg_tonglian
    df.pctchg_wind[w_wsd_data$CODE,1] <- w_wsd_data$PCT_CHG/100
    fwrite(df.pctchg_wind, paste(s.dir.database_wind, s.name,"/",s.date,".csv",sep=""),row.names = T,na="NA")
  }
  for(i.date in 1:length(v.dates_new))
  {
    s.date <- as.character(v.dates_new[i.date])
    print(s.date)
    df.pctchg_wind <- utility.load_data_withoutDate(paste(s.dir.database_wind, s.name,"/",s.date,".csv",sep=""))
    df.volume <- utility.load_data_withoutDate(paste(s.dir.data.daily, "volume/",s.date,".csv",sep=""))
    if(sum(!which(is.na(df.pctchg_wind[,1])) %in% which(is.na(df.volume[,1])|df.volume[,1]==0)))
    {
      stop(s.date)
    }
    file.copy(paste(s.dir.database_wind, s.name,"/",s.date,".csv",sep=""),paste(s.dir.data.daily, s.name,"/",s.date,".csv",sep=""))
  }
  print("涨跌幅数据更新完成")
}

# 成分股权重
for(s.name in c("hs300_constituents","zz500_constituents"))
{
  for(s.date in as.character(v.dates_new))
  {
    if(file.exists(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep="")))
    {
      file.remove(paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))  
    }
    file.copy(paste(s.dir.database_handmade,"lj/",s.name,"/",s.date,".csv",sep="")  ,paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep="")  )
  }
}
print("成分股 正式写入数据库")

s.name <- "price_limit"
for(s.date in as.character(v.dates_new))
{
  df.data <- fread(paste(s.dir.database_handmade,"lj/",s.name,"/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)
  v.tickers <- df.data[,1]
  v.tickers <- v.tickers[df.data[,2]==1]
  if(sum(!df.data[,2]%in%c(1,-1)))
  {
    stop(s.file)
  }
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  v.tickers <- unique(intersect(v.tickers,v.ticker_cur))
  v.volume <- utility.load_data_withoutDate(paste(s.dir.data.daily,"volume/",s.date,".csv",sep=""))[v.tickers,1]
  if(any(is.na(v.volume)|v.volume==0))
  {
    stop(s.date)
  }
  df.result <- data.frame(ticker = v.tickers,stringsAsFactors = F)
  fwrite(df.result, paste(s.dir.database_handmade,"limitUp/",s.date,".csv",sep=""),row.names = F,na="NA")
}
s.name <- "price_limit"
for(s.date in as.character(v.dates_new))
{
  df.data <- fread(paste(s.dir.database_handmade,"lj/",s.name,"/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)
  v.tickers <- df.data[,1]
  v.tickers <- v.tickers[df.data[,2]==-1]
  if(sum(!df.data[,2]%in%c(1,-1)))
  {
    stop(s.file)
  }
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  v.tickers <- unique(intersect(v.tickers,v.ticker_cur))
  v.volume <- utility.load_data_withoutDate(paste(s.dir.data.daily,"volume/",s.date,".csv",sep=""))[v.tickers,1]
  if(any(is.na(v.volume)|v.volume==0))
  {
    stop(s.date)
  }
  df.result <- data.frame(ticker = v.tickers,stringsAsFactors = F)
  fwrite(df.result, paste(s.dir.database_handmade,"limitDown/",s.date,".csv",sep=""),row.names = F,na="NA")
}

for(s.name in c("limitUp","limitDown"))
{
  for(s.date in as.character(v.dates_new))
  {
    v.tickers <- fread(paste(s.dir.database_handmade,s.name,"/",s.date,".csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    v.pctchg <- utility.load_data_withoutDate(paste(s.dir.data.daily,"pct_chg/",s.date,".csv",sep=""))[v.tickers,1]
    print(v.pctchg)
    file.copy(paste(s.dir.database_handmade,s.name,"/",s.date,".csv",sep=""), paste(s.dir.data.stocksnames,s.name,"/",s.date,".csv",sep=""))
  }
}
print("涨跌停的数据 正式写入数据库")


# 次新股
s.name <- "newly_stocks_3Months"
# <= 
for(i.date in 1:length(v.dates_new))
{
  d.date <- v.dates_new[i.date]
  print(d.date)
  v.ticker_cur <- utility.getAllAvailableTicker_new(as.character(d.date))
  v.tickers_newlyStocks <- v.ticker_cur[sapply(df.ticker_info_new_2[v.ticker_cur,"startDate"],function(x){d.date - x <= 59 & d.date >= x})]
  v.days_fromStart <- as.numeric(sapply(df.ticker_info_new_2[v.tickers_newlyStocks,"startDate"],function(x){d.date-x+1}))
  df.data <- data.frame(tradingDates = v.days_fromStart,stringsAsFactors = F)
  rownames(df.data) <- v.tickers_newlyStocks
  fwrite(df.data, paste(s.dir.data.stocksnames,s.name,"/",d.date,".csv",sep=""),row.names = T,na="NA")
  print("新股 正式写入数据库")
}

s.name <- "basic_stock_pool"
for(i.date in 1:length(v.dates_new))
{
  d.date <- v.dates_new[i.date]
  if(!file.exists(paste(s.dir.data.stocksnames,s.name,"/",d.date,".csv",sep="")))
  {
    tryCatch(
      {
        v.ticker_cur <- utility.getAllAvailableTicker_new(as.character(d.date))
        
        # new stocks
        df.tmp <- utility.get_stocknames("newly_stocks_3Months",d.date)
        v.new_stocks <-  df.tmp[df.tmp[,2]<=40,1]
        
        # 停牌股
        df.tmp <- utility.get_stocknames("trading_stopped_stocks",d.date)
        v.stopped_stocks <- df.tmp[df.tmp[,2]>=5,1]
        
        # ST股
        df.tmp <- utility.get_stocknames("ST",d.date)
        v.ST_stocks <- df.tmp[,1]
        
        # 成分股
        v.index_stocks <- NULL
        v.index_stocks <- c(v.index_stocks, utility.get_stocknames("hs300_constituents",d.date)[,1])
        v.index_stocks <- c(v.index_stocks, utility.get_stocknames("zz500_constituents",d.date)[,1])
        
        # 已经公告有退市风险的股票
        # 可以考虑，但暂不考虑
        v.delistedWarning_stocks <- utility.get_stocknames("delist_warning",d.date)[,1]
        
        # basic stock pool
        v.basic_stockpool <- setdiff(v.ticker_cur, setdiff(union_all(v.new_stocks,v.stopped_stocks,v.ST_stocks,v.delistedWarning_stocks),v.index_stocks))
        #v.basic_stockpool <- setdiff(v.basic_stockpool, v.delisted_stocks)
        df.tmp <- data.frame(stocks =v.basic_stockpool ,stringsAsFactors = F)  
        print(length(v.basic_stockpool)/length(v.ticker_cur))
        fwrite(df.tmp, paste(s.dir.data.stocksnames,s.name,"/",d.date,".csv",sep=""),row.names = F,na="NA")    
      },
      error=function(cond)
      {
        message(cond)
      }
    )  
  }
  print("基础券池 正式写入数据库")
}

# 指数数据
for(s.data_name in c("open","close","pct_chg"))
{
  s.file <- paste(s.dir.database_wind,"index_",s.data_name,".csv",sep="")
  df.old <- utility.load_data_new(s.file)
  w_wsd_data <- w.wsd(colnames(df.old),s.data_name,as.character(v.dates_new[1]),tail(v.dates_new,1),"ShowBlank=NA")$Data
  tryCatch(
    {
      df.data <- data.frame(w_wsd_data[,colnames(df.old)],stringsAsFactors = F)    
      rownames(df.data) <- w_wsd_data[,1]
      colnames(df.data) <- colnames(df.old)  
    },
    error = function(cond)
    {
      df.data <<- data.frame(matrix(w_wsd_data[1:dim(df.old)[2],2],nrow=1,ncol=dim(df.old)[2]),stringsAsFactors = F)
      rownames(df.data) <<- s.date
      colnames(df.data) <<- colnames(df.old)
    }
  )
  if(s.data_name == "pct_chg")
  {
    df.data[,1:dim(df.data)[2]] <- df.data[,1:dim(df.data)[2]]/100
  }
  fwrite(df.data,s.file,row.names = T,append = T,na="NA")
}

for(s.data_name in c("open","close","pct_chg"))
{
  s.file <- paste(s.dir.database_wind,"index_",s.data_name,".csv",sep="")
  df.data <- utility.load_data_new(s.file)
  print("指数数据 写入正式数据库")
  fwrite(df.data,paste(s.dir.data.daily,"index_",s.data_name,".csv",sep=""),row.names = T,na="NA")
}
d.date_min <- as.Date("2100-01-01")
v.dirs <- list.dirs(s.dir.data.stocksnames,recursive = F)
for(s.dir in paste(v.dirs,"/",sep=""))
{
  print(paste(s.dir,d.date_min))
  d.date_min <- min(d.date_min,max(as.Date(substring(list.files(s.dir,pattern = ".csv"),1,10))))
}

v.dirs <- list.dirs(s.dir.data.daily,recursive = F)
for(s.dir in setdiff(paste(v.dirs,"/",sep=""),c("D:/data/A/daily//000016/","D:/data/A/daily//000300/",
                                                "D:/data/A/daily//000905/","D:/data/A/daily//000001/")))
{
  print(paste(s.dir,d.date_min))
  d.date_min <- min(d.date_min,max(as.Date(setdiff(substring(list.files(s.dir,pattern = ".csv"),1,10),"date.csv"))))
}
if(d.date_min != max(v.dates_new))
{
  stop("日度数据还没有更新完成")
}else
{
  print("所有日度数据 已经正式写入数据库")  
}
for(s.dir in setdiff(paste(v.dirs,"/",sep=""),c("D:/data/A/daily//000016/","D:/data/A/daily//000300/",
                                                "D:/data/A/daily//000905/","D:/data/A/daily//000001/")))
{
  if(file.exists(paste(s.dir,"date.csv",sep="")))
  {
    file.remove(paste(s.dir,"date.csv",sep=""))
  }
  file.copy(paste(s.dir.data.daily,"date.csv",sep=""),
            paste(s.dir,"date.csv",sep=""))
}

s.dir_origMinuteDate <- "I:/data/minutes/index/2021/"
v.data_classes <- c("open","high","low","close","volume","amount")
v.data_classes_inOrigData <- c("openPrice","highPrice","lowPrice","closePrice","totalVolume","totalValue")
for(s.index in c("000001","000016","000300","000905"))
{
  s.dir.data_this <- paste(s.dir.data.minute,s.index,"/",sep="")
  
  #v.codes <- c(".XSHG_202001.csv",".XSHG_202002.csv",".XSHG_202003.csv",".XSHG_202004.csv",".XSHG_202005.csv",".XSHG_202006.csv")
  #v.codes <- c(".XSHG_202007.csv",".XSHG_202008.csv",".XSHG_202009.csv",".XSHG_202010.csv",".XSHG_202011.csv",".XSHG_202012.csv",".XSHG_202101.csv")
  v.codes <- c(".XSHG_202101.csv",".XSHG_202102.csv",".XSHG_202103.csv",".XSHG_202104.csv")
  for(s.code in v.codes)
  {
    s.file <- paste(s.dir_origMinuteDate,s.index,s.code,sep="") 
    df.data <- fread(s.file,header = T, stringsAsFactors = F, data.table = F)
    v.dates <- sort(as.Date(unique(df.data[,"dataDate"])))[sort(as.Date(unique(df.data[,"dataDate"])))%in%v.dates_new]
    if(length(v.dates))
    {
    for(i.date in 1:length(v.dates))
    {
      d.date <- v.dates[i.date]
      print(d.date)
      df.data_orig <- df.data[df.data[,"dataDate"]==as.character(d.date),]
      df.data_clean <- df.data_orig[-1,v.data_classes_inOrigData]  
      colnames(df.data_clean) <- v.data_classes  
      rownames(df.data_clean) <- v.minutes_DataFormat
      df.data_clean[,"pct_chg"] <- as.numeric(df.data_orig[-1,"closePrice"]/df.data_orig[1:240,"closePrice"] - 1)
      fwrite(df.data_clean,paste(s.dir.data_this,d.date,".csv",sep=""),row.names = T,na="NA")
    }
    }
  }
}

d.date_min <- as.Date("2100-01-01")
v.dirs <- list.dirs(s.dir.data.minute,recursive = F)
for(s.dir in paste(v.dirs,"/",sep=""))
{
  print(s.dir)
  print(max(as.Date(substring(setdiff(list.files(s.dir,pattern = ".csv"),"date.csv"),1,10))))
  d.date_min <- min(d.date_min,max(as.Date(substring(setdiff(list.files(s.dir,pattern = ".csv"),"date.csv"),1,10))))
}
if(d.date_min != max(v.dates_new))
{
  stop("高频数据还没有更新完成")
}else
{
  print("高频数据也更新完毕")
}
fwrite(df.date_new, paste(s.dir.data.minute,"date_minute.csv",sep=""),row.names = F,na="NA")


