
#######以下为基本面因子计算的辅助函数#######

# 仅作保留用
get_s_stm_actual_ISSUINGDATE <- function(l.data)
{
  s.date <- substring(rownames(l.data$close)[1],1,10)
  v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
  v.alpha <- rep(NA,length(v.ticker_cur))
  s.date_str <- str_remove_all(s.date,"-")
  for(i.ticker in 1:length(v.ticker_cur))
  {
    s.ticker <- v.ticker_cur[i.ticker]
    print(s.ticker)
    s.sql <- paste("select max(S_STM_ACTUAL_ISSUINGDATE) from t_ods_t70_AShareIssuingDatePredict where S_INFO_WINDCODE = '",s.ticker,
                   "' and S_STM_ACTUAL_ISSUINGDATE <= '",s.date_str,"'",sep="")
    tryCatch(
      {
        v.alpha[i.ticker] <- fn.RunSql(s.sql)[1,1]    
        print(v.alpha[i.ticker])
      },
      error = function(e)
      {
        message(e)
      }
    )
  }
  return(v.alpha)
}
# 更新DB格式的数据
utility.update_DBData_FromMySQL <- function(s.name)
{
  # 读取现有里面的DB格式
  s.dir_DB <- paste(s.dir.data.fundamental,"DB_",s.name,"/",sep="")
  utility.createFolder(s.dir_DB)
  l.savedData <- utility.loadNewFrameWorka_all(s.name)
  b.updateDate <- F
  if(!file.exists(paste(s.dir_DB,"date.csv",sep="")))
  {
    b.updateDate <- T
  }else
  {
    if(!identical(fread(paste(s.dir_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1],
                  v.dates_fundamental_all))
    {
      b.updateDate <- T
    }
  }
  if(b.updateDate)
  {
    # 读取需要更新的df
    df <- fread(paste(s.dir.data.fundamental,"WINDDB_total/",s.name,".csv",sep=""),
                header = T,
                stringsAsFactors = F,
                data.table = F)
    
    # 根据报告期来划分
    print(head(df))
    v.periods <- as.character(sort(unique(df[,"REPORT_PERIOD"])))
    
    for(s.period in v.periods)
    {
      b.update <- F
      if(!s.period %in%names(l.savedData))
      {
        v.ticker_cur <- utility.getAllAvailableTicker_new(tail(v.dates_fundamental_all,1))
        df.data <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
        rownames(df.data) <- v.ticker_cur
        colnames(df.data) <- s.period
        l.savedData[[s.period]] <- df.data
      }
      df.cur <- df[df[,"REPORT_PERIOD"] == s.period & df[,"S_INFO_WINDCODE"]%in%rownames(l.savedData[[s.period]]),]
      if(dim(df.cur)[1])
      {
        for(i.loc in 1:dim(df.cur)[1])
        {
          s.ticker <- df.cur[i.loc,"S_INFO_WINDCODE"]
          if(length(i.loc)>1)
          {
            stop(s.ticker)
          }
          if(is.na(l.savedData[[s.period]][s.ticker,1])&!is.na(df.cur[i.loc,s.name]))
          {
            print(paste(s.ticker,"更新了数据"))
            b.update <- T
            l.savedData[[s.period]][s.ticker,1] <- df.cur[i.loc,s.name]
          }
        }
      }
      if(b.update)
      {
        fwrite(l.savedData[[s.period]],
               paste(s.dir_DB,"/",s.period,".csv",sep=""),row.names = T,na="NA")
      }
    }
    # 要根据df来判断更新的日期
    # 更新日期
    if(file.exists(paste(s.dir_DB,"date.csv",sep="")))
    {
      file.remove(paste(s.dir_DB,"date.csv",sep=""))
    }
    fwrite(data.frame(date=v.dates_fundamental_all,stringsAsFactors = F),
           paste(s.dir_DB,"date.csv",sep=""),
           row.names = F,
           na = "NA")
  }else
  {
    print("无需更新")
  }
  return(l.savedData)
}

# 从DB格式的数据库里面导入的函数
def_get_data_fromourDB <- function(s.name)
{
  s.name_this <- s.name
  func <- function(l.data)
  {
    s.date <- substring(rownames(l.data$close)[1],1,10)
    v.ticker_cur <- utility.getAllAvailableTicker_new(s.date)
    v.alpha <- rep(NA,length(v.ticker_cur))
    for(i.ticker in 1:length(v.ticker_cur))
    {
      s.ticker <- v.ticker_cur[i.ticker]
      if(!is.na(l.data$S_STM_ACTUAL_ISSUINGDATE_ALL[1,s.ticker]))
      {
        v.alpha[i.ticker] <- utility.loadNewFrameWork(s.name_this,
                                                      as.character(l.data$S_STM_ACTUAL_ISSUINGDATE_ALL[1,s.ticker]),
                                                      s.ticker)  
      }
    }
    return(v.alpha)
  }
  return(func)
}

# 将DB数据转换为日期数据
utility.convertDBdata_toDailyData <- function(obj.fundamental)
{
  s.name <- obj.fundamental@name
  # 把第一天算出来
  utility.updateData_DB_newFrameWork_recursive(obj.fundamental, obj.fundamental@startTimePoint)
  # 把db_data的日期拉出来
  s.file_date <- paste(obj.fundamental@folder_DB,"date.csv",sep="")
  if(!file.exists(s.file_date))
  {
    stop("没有DB相应的数据")
  }
  v.dates_DB <- fread(s.file_date,header = T,stringsAsFactors = F,data.table = F)[,1]
  if(length(setdiff(substring(v.dates_fundamental_all[2:length(v.dates_fundamental_all)],1,10),v.dates_DB)))
  {
    stop("DB数据不足")
  }
  l.savedData <- utility.loadNewFrameWorka_all(s.name)
  s.dir <- obj.fundamental@folder
  for(i.file in 2:length(v.dates_fundamental_all))
  {
    s.file_last <- paste(substring(l.format[["daily"]]@all_timepoints[i.file-1],1,10),".csv",sep="")
    s.file <- paste(substring(l.format[["daily"]]@all_timepoints[i.file],1,10),".csv",sep="")
    if(!file.exists(paste(s.dir,s.file,sep="")))
    {
      print(s.file)
      df.data_last <- utility.load_data_withoutDate(paste(s.dir,s.file_last,sep=""))
      df.updated_cur <- utility.load_data_withoutDate(paste(s.dir.data.fundamental,"S_STM_ACTUAL_ISSUINGDATE/",s.file,sep=""))
      
      v.ticker_cur <- utility.getAllAvailableTicker_new(substring(s.file,1,10))
      df.data <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
      rownames(df.data) <- v.ticker_cur
      colnames(df.data) <- "1500"
      df.data[rownames(df.data_last),1] <- df.data_last[,1]
      if(sum(!is.na(df.updated_cur[,1])))
      {
        v.locs <- which(!is.na(df.updated_cur[,1]))
        for(i.loc in v.locs)
        {
          s.ticker <- rownames(df.updated_cur)[i.loc]
          s.reportDate_cur <- as.character(df.updated_cur[i.loc,1])
          df.data[s.ticker,1] <- l.savedData[[s.reportDate_cur]][s.ticker,1]
        }
      }
      fwrite(df.data, paste(s.dir,s.file,sep=""),row.names = T,na="NA")  
    }
  }
  return(0)
}

# DB格式数据的读取
utility.loadNewFrameWork <- function(s.name,s.reportDate,s.ticker)
{
  if(!s.name %in% names(l.newFrameWork))
  {
    l.newFrameWork[[s.name]] <<- utility.loadNewFrameWorka_all(s.name)
  }
  if(s.reportDate%in% names(l.newFrameWork[[s.name]]))
  {
    return(l.newFrameWork[[s.name]][[s.reportDate]][s.ticker,1])  
  }else
  {
    return(NA)
  }
}
utility.loadNewFrameWorka_all <- function(s.name)
{
  if(s.name %in% names(l.F[["daily"]]))
  {
    s.dir <- l.F[["daily"]][[s.name]]@folder_DB  
  }else
  {
    s.dir <- paste(s.dir.data.factor,"daily/fundamentals_DB/",s.name,"_DB/",sep="")
  }
  if(!dir.exists(s.dir))
  {
    stop(paste(s.name,"报告期格式的数据缺失!"))
  }
  v.all_reportDates <- as.character(sort(substring(setdiff(list.files(s.dir),"date.csv"),1,8)))
  if(length(v.all_reportDates))
  {
    l.result <- vector("list", length(v.all_reportDates))
    names(l.result) <- v.all_reportDates
    for(s.reportDate in v.all_reportDates)
    {
      s.file <- paste(s.dir,s.reportDate,".csv",sep="")
      df_tmp <- utility.load_data_withoutDate(s.file)
      v.ticker_cur <- utility.getAllAvailableTicker_new(tail(v.dates_fundamental_all,1))
      df.data <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
      rownames(df.data) <- v.ticker_cur
      colnames(df.data) <- s.reportDate
      df.data[rownames(df_tmp),1] <- df_tmp[,1]
      l.result[[s.reportDate]] <- df.data
    }
    return(l.result)  
  }else
  {
    return(list())
  }
}

# DB格式数据导出的因子的定义
def_class_forDBdata <- function(s.name,additive = T,duration = "Q",b.fromWIND = T)
{
  # 构建因子
  obj.fundamental <- factor_reportFundamentals()
  obj.fundamental@name <- s.name
  obj.fundamental@factor_calc_type <- "daily_daily"
  obj.fundamental@dependencies <- c("close","S_STM_ACTUAL_ISSUINGDATE_ALL")
  obj.fundamental@span <- c(1,1)
  obj.fundamental@useHigherFreq <- F
  obj.fundamental@isDigital <- F
  obj.fundamental@startTimePoint <- v.all_timepoints[1]
  obj.fundamental@calc_function <- def_get_data_fromourDB(s.name)
  obj.fundamental@additive <- additive
  obj.fundamental@duration <- duration
  if(b.fromWIND)
  {
    obj.fundamental@folder <- paste(s.dir.data.fundamental,s.name,"/",sep="")
    obj.fundamental@folder_DB <- paste(s.dir.data.fundamental,"DB_",s.name,"/",sep="")  
  }else
  {
    obj.fundamental@folder <- paste(s.dir.data.factor,"daily/fundamentals/",s.name,"/",sep="")
    obj.fundamental@folder_DB <- paste(s.dir.data.factor,"daily/fundamentals_DB/",s.name,"_DB/",sep="")  
  }
  return(obj.fundamental)
}
# 自动生成delay
def_delayClassForMonthlyClass <- function(obj.monthly,n.delay = 1)
{
  s.name_this <- paste(obj.monthly@name,"#delay",n.delay,sep="")
  obj.fundamental <- def_class_forDBdata(s.name_this,additive = obj.monthly@additive,duration = obj.monthly@duration,b.fromWIND = F)
  return(obj.fundamental)
}
# 对累计数据因子定义TTM因子
def_TTM_class_forMonthlyClass <- function(obj.alpha)
{
  s.name <- obj.alpha@name
  s.name_this <- paste(s.name,"_TTM",sep="")
  obj.fundamental <- def_class_forDBdata(s.name_this,additive = F,duration = "TTM",b.fromWIND = F)
  return(obj.fundamental)
}
# 从因子里面定义累计数据因子
def_accum_class_forMonthlyClass <- function(obj.alpha)
{
  s.name <- obj.alpha@name
  s.name_this <- paste(s.name,"#accum",sep="")
  obj.fundamental <- def_class_forDBdata(s.name_this,additive = F,duration = "THISYEAR",b.fromWIND = F)
  return(obj.fundamental)
}
def_del_class_forMonthlyClass <- function(obj.alpha)
{
  s.name <- obj.alpha@name
  s.name_this <- paste(s.name,"#del",sep="")
  obj.fundamental <- def_class_forDBdata(s.name_this,additive = T,duration = "Q",b.fromWIND = F)
  return(obj.fundamental)
}

# DB格式数据的计算
utility.accumulatedData_fromDB <- function(obj.result,obj.from)
{
  # 根据报告期来算
  s.name_this <- obj.result@name
  s.dir <- obj.result@folder_DB
  utility.createFolder(s.dir)
  b.needUpdate <- T
  if(file.exists(paste(s.dir,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(s.dir,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    v.dates_1 <- fread(paste(obj.from@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    if(identical(v.dates_result,v.dates_1))
    {
      print("无需更新")
      b.needUpdate <- F
    }
  }
  if(b.needUpdate)
  {
    l.savedData <- utility.loadNewFrameWorka_all(obj.from@name)
    for(s.reportDate in names(l.savedData))
    {
      if(substring(s.reportDate,5,8)=="0331")
      {
        v.reportDates_considered <- c(s.reportDate)
      }else if(substring(s.reportDate,5,8)=="0630")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0331","0630"),sep="")
      }else if(substring(s.reportDate,5,8)=="0930")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0331","0630","0930"),sep="")
      }else if(substring(s.reportDate,5,8)=="1231")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0331","0630","0930","1231"),sep="")
      }
      df <- data.frame(matrix(0,nrow=length(v.ticker_all),ncol = 1),stringsAsFactors = F)
      rownames(df) <- v.ticker_all
      colnames(df) <- "data"
      for(s.reportDate_cur in v.reportDates_considered)
      {
        if(s.reportDate_cur%in%names(l.savedData))
        {
          print(s.reportDate_cur)
          df[rownames(l.savedData[[s.reportDate_cur]]),1] <- 
            df[rownames(l.savedData[[s.reportDate_cur]]),1] + 
            l.savedData[[s.reportDate_cur]][,1]
        }
      }
      fwrite(df, paste(s.dir,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
    {
      file.remove(paste(obj.result@folder_DB,"date.csv",sep=""))
    }
    file.copy(paste(obj.from@folder_DB,"date.csv",sep=""),
              paste(obj.result@folder_DB,"date.csv",sep=""))
  }else
  {
    print("无需更新")
  }
}

utility.getQFAfromFA <- function(obj.result,obj.from)
{
  s.name_this <- obj.result@name
  s.dir <- obj.result@folder_DB
  utility.createFolder(s.dir)
  b.needUpdate <- T
  if(file.exists(paste(s.dir,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(s.dir,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    v.dates_1 <- fread(paste(obj.from@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    if(identical(v.dates_result,v.dates_1))
    {
      print("无需更新")
      b.needUpdate <- F
    }
  }
  if(b.needUpdate)
  {
    l.savedData <- utility.loadNewFrameWorka_all(obj.from@name)
    for(s.reportDate in names(l.savedData))
    {
      if(substring(s.reportDate,5,8)=="0331")
      {
        v.reportDates_considered <- c(s.reportDate)
      }else if(substring(s.reportDate,5,8)=="0630")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0331","0630"),sep="")
      }else if(substring(s.reportDate,5,8)=="0930")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0630","0930"),sep="")
      }else if(substring(s.reportDate,5,8)=="1231")
      {
        v.reportDates_considered <- paste(substring(s.reportDate,1,4),c("0930","1231"),sep="")
      }
      df <- data.frame(matrix(0,nrow=length(rownames(l.savedData[[v.reportDates_considered[1]]])),ncol = 1),stringsAsFactors = F)
      rownames(df) <- rownames(l.savedData[[v.reportDates_considered[1]]])
      colnames(df) <- "data"
      if(length(v.reportDates_considered)==1)
      {
        df[rownames(l.savedData[[v.reportDates_considered[1]]]),1] <- 
          l.savedData[[v.reportDates_considered[1]]][,1]
      }else
      {
        if(v.reportDates_considered[1]%in%names(l.savedData))
        {
          df[rownames(l.savedData[[v.reportDates_considered[2]]]),1] <- 
            l.savedData[[v.reportDates_considered[2]]][,1] - 
            l.savedData[[v.reportDates_considered[1]]][,1]  
        }
      }
      fwrite(df, paste(s.dir,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
    {
      file.remove(paste(obj.result@folder_DB,"date.csv",sep=""))
    }
    file.copy(paste(obj.from@folder_DB,"date.csv",sep=""),
              paste(obj.result@folder_DB,"date.csv",sep=""))  
  }
}
utility.DBDATA_calc <- function(obj.alpha_1,obj.alpha_2,dir_result,func)
{
  utility.createFolder(dir_result)
  v.dates_1 <- fread(paste(obj.alpha_1@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_2 <- fread(paste(obj.alpha_2@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  if(!identical(v.dates_1,v.dates_2))
  {
    stop("两个DBdata的数据不一样")
  }
  b.needUpdate <- T
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    if(identical(v.dates_result, v.dates_1))
    {
      print("无需更新")
      b.needUpdate <- F
    }
  }
  if(b.needUpdate)
  {
    l.list_1 <- utility.loadNewFrameWorka_all(obj.alpha_1@name)
    l.list_2 <- utility.loadNewFrameWorka_all(obj.alpha_2@name)
    if(!identical(names(l.list_1),
                  names(l.list_2)))
    {
      print("两个DBdata的报告期不一样")
    }
    v.names_intersect <- intersect(names(l.list_1),
                                   names(l.list_2))
    if(length(v.names_intersect))
    {
      for(s.reportDate in v.names_intersect)
      {
        print(s.reportDate)
        v.result <- func(l.list_1[[s.reportDate]][,1],l.list_2[[s.reportDate]][,1])
        df <- data.frame(data = v.result,stringsAsFactors = F)
        rownames(df) <- rownames(l.list_1[[s.reportDate]])
        colnames(df) <- s.reportDate
        fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
      }  
    }else
    {
      stop("两个DBdata的报告期没有交集")
    }
    
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha_1@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep="")) 
  }
  
  return(0)
}
utility.DBDATA_calc_3 <- function(obj.alpha_1,obj.alpha_2,obj.alpha_3,dir_result,func)
{
  utility.createFolder(dir_result)
  v.dates_1 <- fread(paste(obj.alpha_1@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_2 <- fread(paste(obj.alpha_2@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_3 <- fread(paste(obj.alpha_3@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  if(!(identical(v.dates_1,v.dates_2)&identical(v.dates_2,v.dates_3)))
  {
    stop("三个DBdata的数据不一样")
  }
  b.needUpdate <- T
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
    if(identical(v.dates_result, v.dates_1))
    {
      print("无需更新")
      b.needUpdate <- F
    }
  }
  if(b.needUpdate)
  {
    l.list_1 <- utility.loadNewFrameWorka_all(obj.alpha_1@name)
    l.list_2 <- utility.loadNewFrameWorka_all(obj.alpha_2@name)
    l.list_3 <- utility.loadNewFrameWorka_all(obj.alpha_3@name)
    if(!(identical(names(l.list_1),
                   names(l.list_2))&identical(
                     names(l.list_2),
                     names(l.list_3))))
    {
      print("三个DBdata的报告期不一样")
    }
    v.names_intersect <- intersect(intersect(names(l.list_1),names(l.list_2)),names(l.list_3))
    if(length(v.names_intersect))
    {
      for(s.reportDate in v.names_intersect)
      {
        print(s.reportDate)
        v.result <- func(l.list_1[[s.reportDate]][,1],l.list_2[[s.reportDate]][,1],l.list_3[[s.reportDate]][,1])
        df <- data.frame(data = v.result,stringsAsFactors = F)
        rownames(df) <- rownames(l.list_1[[s.reportDate]][,1])
        colnames(df) <- s.reportDate
        fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
      } 
    }else
    {
      stop("三个DBdata的报告期没有交集")
    }
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha_1@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep="")) 
  }
  return(0)
}
utility.DBDATA_delay <- function(obj.alpha,dir_result,delay = 1)
{
  utility.createFolder(dir_result)
  v.dates_orig <- fread(paste(obj.alpha@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_delay <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_orig, v.dates_delay)
  }else
  {
    v.dates_undone <- v.dates_orig
  }
  if(length(v.dates_undone))# 需要重算时，整个重新算
  {
    l.list_orig <- utility.loadNewFrameWorka_all(obj.alpha@name)
    for(s.reportDate in names(l.list_orig))
    {
      print(s.reportDate)
      s.lastReportDate <- utility.get_lastReportDate_multi(s.reportDate,delay)
      print(s.lastReportDate)
      v.ticker_cur <- utility.getAllAvailableTicker_new(tail(v.dates_fundamental_all,1))
      if(s.lastReportDate %in% names(l.list_orig))
      {
        df <- l.list_orig[[s.lastReportDate]]
      }else
      {
        df <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
      }
      rownames(df) <- v.ticker_cur
      colnames(df) <- s.reportDate
      fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep=""))
    return(0)  
  }else
  {
    print("无需更新")
  }
}
utility.DBDATA_growth <- function(obj.alpha,dir_result,n.delay = 4,func=abs)
{
  utility.createFolder(dir_result)
  v.dates_orig <- fread(paste(obj.alpha@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_delay <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_orig, v.dates_delay)
  }else
  {
    v.dates_undone <- v.dates_orig
  }
  if(length(v.dates_undone))# 需要重算时，整个重新算
  {
    l.list_orig <- utility.loadNewFrameWorka_all(obj.alpha@name)
    for(s.reportDate in names(l.list_orig))
    {
      print(s.reportDate)
      s.lastReportDate <- utility.get_lastReportDate_multi(s.reportDate,n.delay)
      print(s.lastReportDate)
      df <- data.frame(matrix(NA,nrow=length(rownames(l.list_orig[[s.reportDate]])),ncol = 1),stringsAsFactors = F)
      if(s.lastReportDate %in% names(l.list_orig))
      {
        df[,1] <- (l.list_orig[[s.reportDate]][,1]-l.list_orig[[s.lastReportDate]][,1])/func(l.list_orig[[s.lastReportDate]][,1])
      }
      rownames(df) <- rownames(l.list_orig[[s.reportDate]])
      colnames(df) <- s.reportDate
      fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep=""))
    return(0)  
  }else
  {
    print("无需更新")
  }
}
utility.calculateDelay_DB <- function(obj.orig,n.delay = 1)
{
  obj.delay <- def_delayClassForMonthlyClass(obj.orig,n.delay)
  print(obj.delay@name)
  
  utility.DBDATA_delay(obj.orig,
                       obj.delay@folder_DB,
                       n.delay)
  utility.convertDBdata_toDailyData(obj.delay)# 这个需要能算出第一天的数据。如果因子本身不能算出，则不能使用该算法。  return(obj.delay)
  return(obj.delay)
}

utility.calculateDelay_data <- function(obj.orig,n.delay = 1)
{
  obj.delay <- def_delayClassForMonthlyClass(obj.orig,n.delay)
  print(obj.delay@name)
  
  utility.DBDATA_delay(obj.orig,
                       obj.delay@folder_DB,
                       n.delay)
  utility.convertDBdata_toDailyData(obj.delay)# 这个需要能算出第一天的数据。如果因子本身不能算出，则不能使用该算法。
  utility.calculateFactor(obj.delay)
  return(obj.delay)
}
utility.calculateTTMforQ <- function(obj.result,obj.from)
{
  utility.createFolder(obj.result@folder_DB)
  v.dates_from <- fread(paste(obj.from@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(obj.result@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_from, v.dates_result)
  }else
  {
    v.dates_undone <- v.dates_from
  }
  if(length(v.dates_undone))
  {
    l.list_from <- utility.loadNewFrameWorka_all(obj.from@name)
    for(s.reportDate in names(l.list_from))
    {
      print(s.reportDate)
      s.reportDate_last1 <- utility.get_lastReportDate_multi(s.reportDate,1)
      s.reportDate_last2 <- utility.get_lastReportDate_multi(s.reportDate,2)
      s.reportDate_last3 <- utility.get_lastReportDate_multi(s.reportDate,3)
      print(s.reportDate_last1)
      print(s.reportDate_last2)
      print(s.reportDate_last3)
      df <- data.frame(matrix(NA,nrow=length(v.ticker_all),ncol = 1),stringsAsFactors = F)
      rownames(df) <- v.ticker_all
      colnames(df) <- s.reportDate
      if(s.reportDate_last1%in%names(l.list_from)&
         s.reportDate_last2%in%names(l.list_from)&
         s.reportDate_last3%in%names(l.list_from))
      {
        df[,1] <- l.list_from[[s.reportDate_last3]][,1] + 
          l.list_from[[s.reportDate_last2]][,1] + 
          l.list_from[[s.reportDate_last1]][,1] + 
          l.list_from[[s.reportDate]][,1]
      }
      fwrite(df,paste(obj.result@folder_DB,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    
    if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
    {
      file.remove(paste(obj.result@folder_DB,"date.csv",sep=""))
    }
    file.copy(paste(obj.from@folder_DB,"date.csv",sep=""),
              paste(obj.result@folder_DB,"date.csv",sep="")) 
  }else
  {
    print("无需更新")
  }
  return(0)
}
utility.calculateTTMforTHISYEAR <- function(obj.result,obj.from)
{
  utility.createFolder(obj.result@folder_DB)
  v.dates_from <- fread(paste(obj.from@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
  {
    v.dates_result <- fread(paste(obj.result@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_from, v.dates_result)
  }else
  {
    v.dates_undone <- v.dates_from
  }
  if(length(v.dates_undone))
  {
    l.list_from <- utility.loadNewFrameWorka_all(obj.from@name)
    for(s.reportDate in names(l.list_from))
    {
      print(s.reportDate)
      df <- data.frame(matrix(NA,nrow=length(rownames(l.list_from[[s.reportDate]])),ncol = 1),stringsAsFactors = F)
      rownames(df) <- rownames(l.list_from[[s.reportDate]])
      colnames(df) <- s.reportDate
      if(substring(s.reportDate,5,8)=="1231")
      {
        df[,1] <- l.list_from[[s.reportDate]][,1]
      }else
      {
        s.reportDate_lastyear <- utility.get_lastReportDate_multi(s.reportDate,4)
        s.reportDate_lastestYearReport <- utility.get_latestYearReportDate(s.reportDate)
        print(s.reportDate_lastyear)
        print(s.reportDate_lastestYearReport)
        if(s.reportDate_lastyear%in%names(l.list_from) &
           s.reportDate_lastestYearReport%in%names(l.list_from))
        {
          df[,1] <- l.list_from[[s.reportDate]][,1] + 
            (l.list_from[[s.reportDate_lastestYearReport]][,1] - l.list_from[[s.reportDate_lastyear]][,1])
        }
      }
      fwrite(df,paste(obj.result@folder_DB,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(obj.result@folder_DB,"date.csv",sep="")))
    {
      file.remove(paste(obj.result@folder_DB,"date.csv",sep=""))
    }
    file.copy(paste(obj.from@folder_DB,"date.csv",sep=""),
              paste(obj.result@folder_DB,"date.csv",sep="")) 
  }else
  {
    print("无需更新")
  }
  return(0)
}
utils.division <- function(v.data_1, v.data_2)
{
  return(v.data_1/v.data_2)
}
utils.addition <- function(v.data_1, v.data_2)
{
  return(v.data_1+v.data_2)
}
utils.multiplication <- function(v.data_1, v.data_2)
{
  return(v.data_1 * v.data_2)
}
utils.multiplication_100 <- function(v.data_1, v.data_2)
{
  return(v.data_1 * v.data_2 * 100)
}
utils.assetturnover_q <- function(v.data_1,v.data_2,v.data_3)
{
  return(2*v.data_1/(v.data_2+v.data_3))
}

# 报告期操作的函数
utility.get_lastReportDate <- function(s.reportDate_cur)
{
  # 上一个报告期
  n.year <- as.integer(substring(s.reportDate_cur,1,4))
  if(substring(s.reportDate_cur,5,8)=="0331")
  {
    s.lastReportDate <- paste(n.year-1,"1231",sep="")
  }else if (substring(s.reportDate_cur,5,8)=="0630")
  {
    s.lastReportDate <- paste(n.year,"0331",sep="")
  }else if (substring(s.reportDate_cur,5,8)=="0930")
  {
    s.lastReportDate <- paste(n.year,"0630",sep="")
  }else if (substring(s.reportDate_cur,5,8)=="1231")
  {
    s.lastReportDate <- paste(n.year,"0930",sep="")
  }
  return(s.lastReportDate)
}

utility.get_lastReportDate_multi <- function(s.reportDate_cur,delay=1)
{
  s.lastResult <- utility.get_lastReportDate(s.reportDate_cur)
  if(delay>1)
  {
    for(i in 2:delay)
    {
      s.lastResult <- utility.get_lastReportDate(s.lastResult)
    }
  }
  return(s.lastResult)
}

utility.get_latestYearReportDate <- function(s.reportDate_cur)
{
  # 上一个报告期
  n.year <- as.integer(substring(s.reportDate_cur,1,4))
  if(substring(s.reportDate_cur,5,8)=="1231")
  {
    return(s.reportDate_cur)
  }else
  {
    return(paste(n.year-1,"1231",sep=""))
  }
}



utility.update_DBData_FromMySQL_new <- function(scheme,v.names,s.lastUpdateDate,s.today,
                                                s.updateToDate,extraCondition)
{
  s.sql <- "select S_INFO_WINDCODE,REPORT_PERIOD,ANN_DT,BUSI_DATE,"
  for(s.name in v.names)
  {
    if(s.name == tail(v.names,1))
    {
      s.sql <- paste(s.sql,s.name,sep="")
    }else
    {
      s.sql <- paste(s.sql,s.name,",",sep="")
    }
  }
  s.sql <- paste(s.sql," from ",scheme,
                 " where ANN_DT >= '",s.lastUpdateDate,"'",
                 " and ANN_DT < '",s.updateToDate,"'",extraCondition,sep="")
  print(s.sql)
  df = fn.RunSql(s.sql)
  df <- subset.data.frame(df,subset = substring(df[,"S_INFO_WINDCODE"],1,1)!="A")
  print(head(df))
  if(any(df$BUSI_DATE!=s.today))
  {
    print("有busi_date不是today的数据")
  }
  utility.createFolder(paste(s.dir.data.WINDDB_update,scheme,"/",sep=""))
  s.file <- paste(s.dir.data.WINDDB_update,scheme,"/",s.today,".csv",sep="")
  fwrite(df,s.file,row.names = T,na="NA")
  
  for(s.name in v.names)
  {
    print(s.name)
    df.subset <- subset.data.frame(df,select = c("S_INFO_WINDCODE","REPORT_PERIOD","ANN_DT","BUSI_DATE",s.name))
    utility.createFolder(paste(s.dir.data.WINDDB_update,s.name,"/",sep=""))
    s.file <- paste(s.dir.data.WINDDB_update,s.name,"/",s.today,".csv",sep="")
    fwrite(df.subset,s.file,row.names = T,na="NA")
  }
  return(0)
}

utility.DBDATA_lastAnnualData <- function(obj.alpha,dir_result)
{
  utility.createFolder(dir_result)
  v.dates_orig <- fread(paste(obj.alpha@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_delay <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_orig, v.dates_delay)
  }else
  {
    v.dates_undone <- v.dates_orig
  }
  if(length(v.dates_undone))# 需要重算时，整个重新算
  {
    l.list_orig <- utility.loadNewFrameWorka_all(obj.alpha@name)
    for(s.reportDate in names(l.list_orig))
    {
      print(s.reportDate)
      v.ticker_cur <- utility.getAllAvailableTicker_new(tail(v.dates_fundamental_all,1))
      if(substring(s.reportDate,5,8)=="1231")
      {
        df <- l.list_orig[[s.reportDate]]
      }else
      {
        s.lastAnnualDate <- paste(as.integer(substring(s.reportDate,1,4))-1,"1231",sep="")
        print(s.lastAnnualDate)
        if(s.lastAnnualDate %in% names(l.list_orig))
        {
          df <- l.list_orig[[s.lastAnnualDate]]  
        }else
        {
          df <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
        }
      }
      rownames(df) <- v.ticker_cur
      colnames(df) <- s.reportDate
      fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep=""))
    return(0)  
  }else
  {
    print("无需更新")
  }
}

utility.DBDATA_5yearsGrowth <- function(obj.alpha,dir_result)
{
  utility.createFolder(dir_result)
  v.dates_orig <- fread(paste(obj.alpha@folder_DB,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]
  v.dates_undone <- NULL
  if(file.exists(paste(dir_result,"date.csv",sep="")))
  {
    v.dates_delay <- fread(paste(dir_result,"date.csv",sep=""),header = T,stringsAsFactors = F,data.table = F)[,1]  
    v.dates_undone <- setdiff(v.dates_orig, v.dates_delay)
  }else
  {
    v.dates_undone <- v.dates_orig
  }
  if(length(v.dates_undone))# 需要重算时，整个重新算
  {
    l.list_orig <- utility.loadNewFrameWorka_all(obj.alpha@name)
    for(s.reportDate in names(l.list_orig))
    {
      print(s.reportDate)
      v.ticker_cur <- utility.getAllAvailableTicker_new(tail(v.dates_fundamental_all,1))
      df <- data.frame(matrix(NA,nrow=length(v.ticker_cur),ncol = 1),stringsAsFactors = F)
      rownames(df) <- v.ticker_cur
      colnames(df) <- s.reportDate
      if(substring(s.reportDate,5,8)=="1231")
      {
        # 要获得过去五年的数据
        v.lastFiveYears <- paste(as.integer(substring(s.reportDate,1,4)) - seq(4,0,-1),"1231",sep="")
        if(sum(v.lastFiveYears %in% names(l.list_orig))>=3)
        {
          for(s.ticker in v.ticker_cur)
          {
            v.data <- rep(NA,length(v.lastFiveYears))
            for(i.year_end in 1:length(v.lastFiveYears))
            {
              s.year_end <- v.lastFiveYears[i.year_end]
              if(s.year_end%in%names(l.list_orig))
              {
                v.data[i.year_end] <- l.list_orig[[s.year_end]][s.ticker,1]
              }
            }
            print(v.data)
            f.ratio <- NA
            df.data <- data.frame(y=v.data,x=seq(1,5,1))
            df.data_naomit <- na.omit(df.data)
            if(dim(df.data_naomit)[1]>=3)
            {
              fit <- lm(y~x,df.data_naomit)
              f.slope <- as.numeric(fit$coefficients["x"])
              f.average <- mean(df.data_naomit[,"y"],na.rm = T)
              f.ratio <- f.slope / f.average
              df[s.ticker,1] <- f.ratio
            }
          }
        }
      }else # 不需要实际计算，取之前的数据
      {
        s.lastAnnualDate <- paste(as.integer(substring(s.reportDate,1,4))-1,"1231",sep="")
        print(s.lastAnnualDate)
        if(file.exists(paste(dir_result,s.lastAnnualDate,".csv",sep="")))
        {
          df <- utility.load_data_withoutDate(paste(dir_result,s.lastAnnualDate,".csv",sep=""))
          df[setdiff(v.ticker_cur,rownames(df)),1] <- NA
        }
      }
      
      fwrite(df,paste(dir_result,s.reportDate,".csv",sep=""),row.names = T,na="NA")
    }
    if(file.exists(paste(dir_result,"date.csv",sep="")))
    {
      file.remove(paste(dir_result,"date.csv",sep=""))
    }
    file.copy(paste(obj.alpha@folder_DB,"date.csv",sep=""),
              paste(dir_result,"date.csv",sep=""))
    return(0)  
  }else
  {
    print("无需更新")
  }
}
