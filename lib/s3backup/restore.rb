require 's3backup/s3wrapper'
require 's3backup/s3log'
require 's3backup/manager'
module S3backup
  class Restore
    def initialize(output_dir,config)
      check_config(config)
      @output_dir = output_dir
      @directories = config["directories"]
      begin
        @s3_obj = S3Wrapper.new(config,false)
      rescue => err
        S3log.error(err.backtrace.join("\n")+"\n"+err.message)
        exit -1
      end
      @manager = Manager.new(@s3_obj,config)
    end
    def check_config(config)
      if config["directories"] 
        if config["directories"].class != Array 
          dir = config["directories"] 
          config["directories"] = Array.new
          config["directories"].push dir
        end
        config["directories"] = config["directories"].map{|d| d=~/\/$/ ? d.chop : d}
      end
    end
    def start
      begin
        @directories = @manager.get_target_bases unless @directories
        @directories.each do |dir|
          @manager.restore(dir,@output_dir)
        end
      rescue => err
        S3log.error(err.backtrace.join("\n")+"\n"+err.message)
        exit -1
      end
    end
  end
end
