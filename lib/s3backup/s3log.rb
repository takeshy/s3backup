require 'logger'
module S3backup
  class S3log
    @@log_file = nil
    @@debug = false
    def S3log.get_logger
      unless @@log_file 
        @@log_file = Logger.new($stderr) 
        @@log_file.level = Logger::INFO
      end
      return @@log_file
    end
    def S3log.set_level(level)
      unless @@debug 
        case level
        when /debug/i
          get_logger.level = Logger::DEBUG
        when /info/i
          get_logger.level = Logger::INFO
        when /warn/i
          get_logger.level = Logger::WARN
        when /error/i
          get_logger.level = Logger::ERROR
        end
      end
    end
    def S3log.set_debug(flg)
      @@debug=flg
      if @@debug 
        get_logger.level = Logger::DEBUG
      end
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
      get_logger.debug(str)
    end
    def S3log.set_logfile(f)
      @@log_file = Logger.new(f)
    end
  end
end
