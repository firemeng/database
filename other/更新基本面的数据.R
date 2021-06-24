######### 初次更新
#s.lastUpdateDate <- "20090101"
#s.lastTradeDate <- "2020-12-30"
#s.today <- "2020-12-31"
#s.updateToDate <- "20210104"
######### 保存
#s.lastUpdateDate <- "20210105"
#s.lastTradeDate <- "2021-01-04"
#s.today <- "2021-01-05"
#s.updateToDate <- "20210106"

# 更新到2月5日
# v.locs_needUpdate <- which(v.dates_all>as.Date("2021-01-05") & v.dates_all < tail(v.dates_all,1))

# 更新到2月9日
#s.updateToDate <- "20210208"
#v.locs_needUpdate <- which(v.dates_all>as.Date("2021-02-05") & v.dates_all < tail(v.dates_all,1))

s.today <- "2021-04-19"
s.lastTradeDate <- "2021-04-16"
s.lastUpdateDate <- "20210419" # 上一个s.updateToDate
s.updateToDate <- "20210420" # 下一个交易日
print(paste("s.today:",s.today))
print(paste("s.lastTradeDate:",s.lastTradeDate))
print(paste("s.lastUpdateDate:",s.lastUpdateDate))
print(paste("s.updateToDate:",s.updateToDate))
  
  
# 基本面数据的更新
# 最重要的是有没有发布新的公告
s.dir.data.fundamental <- paste(s.data_root_dir,"A/fundamentals/",sep="")
source(paste(s.code_root_dir,"fa_db_client.R",sep=""),encoding = "utf-8")
source(paste(s.code_root_dir,"new_framework_utils.R",sep=""),encoding = "utf-8")
s.dir.data.WINDDB_update <- paste(s.dir.data.fundamental,"WINDDB_update/",sep="")
utility.createFolder(s.dir.data.WINDDB_update)
s.name <- "s_stm_actual_issuingdate"
{
  s.sql <- paste("select S_INFO_WINDCODE,REPORT_PERIOD,S_STM_ACTUAL_ISSUINGDATE,BUSI_DATE from t_ods_t70_AShareIssuingDatePredict where",
                 " S_STM_ACTUAL_ISSUINGDATE>='",s.lastUpdateDate,"' and S_STM_ACTUAL_ISSUINGDATE<'",
                 s.updateToDate,"'",sep="")
  print(s.sql)
  df = fn.RunSql(s.sql)
  if(any(df$BUSI_DATE!=s.today))# 大数据更新我们的数据时，是会删掉全部的数据，再重新更新。所以所有的BUSI_DATE都会更新
  {
    print("有busi_date不是today的数据")
  }
  utility.createFolder(paste(s.dir.data.WINDDB_update,s.name,"/",sep=""))
  s.file <- paste(s.dir.data.WINDDB_update,s.name,"/",s.today,".csv",sep="")
  fwrite(df,s.file,row.names = T,na="NA")
}

# t_ods_t70_AShareFinancialIndicator 这一张表的数据
v.names <- c("S_QFA_DEDUCTEDPROFIT",
             "S_QFA_DEDUCTEDPROFITTOPROFIT",
             "S_FA_OCFTOOR",
             "S_FA_FCFF",
             "S_FA_CFPS",
             "S_FA_EBIT",
             "S_FA_CURRENT",
             "S_FA_ORPS",
             "S_FA_EXTRAORDINARY",
             "S_FA_DEDUCTEDPROFIT")
extraCondition <- ""
scheme <- "t_ods_t70_AShareFinancialIndicator"
utility.update_DBData_FromMySQL_new(scheme,
                                    v.names,
                                    s.lastUpdateDate,
                                    s.today,
                                    s.updateToDate,
                                    extraCondition)

# t_ods_t70_AShareBalanceSheet 这一张表的数据
v.names <- c("TOT_SHRHLDR_EQY_EXCL_MIN_INT",
             "TOT_ASSETS")
extraCondition <- " and STATEMENT_TYPE = '408001000'"
scheme <- "t_ods_t70_AShareBalanceSheet"
utility.update_DBData_FromMySQL_new(scheme,
                                    v.names,
                                    s.lastUpdateDate,
                                    s.today,
                                    s.updateToDate,
                                    extraCondition)

# t_ods_t70_AShareBalanceSheet 这一张表的数据
v.names <- c("OPER_REV",
             "LESS_OPER_COST")
extraCondition <- " and STATEMENT_TYPE = '408001000'"
scheme <- "t_ods_t70_AShareIncome"
utility.update_DBData_FromMySQL_new(scheme,
                                    v.names,
                                    s.lastUpdateDate,
                                    s.today,
                                    s.updateToDate,
                                    extraCondition)
# 将单日更新的信息合并
v.names <- c("s_stm_actual_issuingdate",
             "S_QFA_DEDUCTEDPROFIT",
             "S_QFA_DEDUCTEDPROFITTOPROFIT",
             "S_FA_OCFTOOR",
             "S_FA_FCFF",
             "S_FA_CFPS",
             "S_FA_EBIT",
             "S_FA_CURRENT",
             "S_FA_ORPS",
             "S_FA_EXTRAORDINARY",
             "S_FA_DEDUCTEDPROFIT",
             "TOT_SHRHLDR_EQY_EXCL_MIN_INT",
             "TOT_ASSETS",
             "OPER_REV",
             "LESS_OPER_COST")
for(s.name in v.names)
{
  print(s.name)
  utility.createFolder(paste(s.dir.data.fundamental,"WINDDB_total/",sep=""))
  s.file_combine <- paste(s.dir.data.fundamental,"WINDDB_total/",s.name,".csv",sep="")
  if(!file.exists(s.file_combine)) # 之前的不存在
  {
    df.data <- fread(paste(s.dir.data.WINDDB_update,s.name,"/",s.today,".csv",sep=""),
                     header = T,
                     stringsAsFactors = F,
                     data.table = F)
    if(length(intersect(c("S_STM_ACTUAL_ISSUINGDATE","ANN_DT"),colnames(df.data))))
    {
      for(s.name_date in intersect(c("S_STM_ACTUAL_ISSUINGDATE","ANN_DT"),colnames(df.data)))
      {
        df.data[,s.name_date] <- 
          sapply(df.data[,s.name_date],utility.convertToDateFormat)  
      }
    }
    fwrite(df.data,s.file_combine,row.names = F,na="NA")
    # copy
    utility.createFolder(paste(s.dir.data.fundamental,"WINDDB_total/copy/",sep=""))
    if(file.exists(paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep="")))
    {
      file.remove(paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep=""))
    }
    file.copy(s.file_combine,
              paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep=""))
  }else
  {
    df.data_new <- fread(paste(s.dir.data.WINDDB_update,s.name,"/",s.today,".csv",sep=""),
                     header = T,
                     stringsAsFactors = F,
                     data.table = F)
    df.data_toCombine <- fread(s.file_combine,
                         header = T,
                         stringsAsFactors = F,
                         data.table = F)
    print(head(df.data_new))
    if(!"BUSI_DATE"%in%colnames(df.data_toCombine))
    {
      df.data_toCombine[,"BUSI_DATE"] <- "NA"  
    }
    if(dim(df.data_new)[2]!=dim(df.data_toCombine)[2])
    {
      stop("新老两个data.frame的列不一样")
    }
    df.data_new <- df.data_new[,colnames(df.data_toCombine)]
    if(!identical(colnames(df.data_new),colnames(df.data_toCombine)))
    {
      stop("新老两个data.frame的列不一样-2")
    }
    df.data_combined <- rbind(df.data_toCombine, df.data_new)
    if(file.exists(paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep="")))
    {
      file.remove(paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep=""))
    }
    file.copy(s.file_combine,
              paste(s.dir.data.fundamental,"WINDDB_total/copy/",s.name,".csv",sep=""))
    fwrite(df.data_combined,s.file_combine,row.names = F,na="NA")
  }
}

# 更新fundamental里面的date
v.dates_new <- sort(unique(c(fread(paste(s.dir.data.fundamental,"date.csv",sep=""),header = T,stringsAsFactors = F,
      data.table = F)[,1],s.today)))
fwrite(data.frame(date=v.dates_new,stringsAsFactors = F),
       paste(s.dir.data.fundamental,"date.csv",sep=""),
       row.names = F,
       na="NA")
