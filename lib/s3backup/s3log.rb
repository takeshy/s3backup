require 'logger'
module S3backup
  class S3log
    @@log_file = nil
    @@debug = false
    def S3log.get_logger
      @@log_file ? @@log_file : Logger.new($stderr) 
    end
    def S3log.set_debug(flg)
      @@debug=flg
    end
    def S3log.error(str)
      get_logger.error(str)
    end
    def S3log.info(str)
      get_logger.info(str)
    end
    def S3log.warn(str)
      get_logger.warn(str)
    end
    def S3log.debug(str)
      if @@debug
        get_logger.debug(str)
      end
    end
    def S3log.set_logfile(f)
      @@log_file = Logger.new(f)
    end
  end
end
