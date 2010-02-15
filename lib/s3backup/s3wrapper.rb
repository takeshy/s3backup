require 'cgi'
require 'aws/s3'
module S3backup
  class S3Wrapper
    attr_reader :bucket
    DEFAULT_MAX_RETRY_COUNT = 30
    def initialize(config,create_flg)
      @bucket= nil
      @s3objects =nil
      #設定ファイルの内容をメンバ変数にセット
      set_config(config)
      #設定ファイルの内容をメンバ変数にセット
      args = {
        :access_key_id     => @access_key_id, 
        :secret_access_key => @secret_access_key
      }
      if @proxy.size != 0
        args[:proxy] = @proxy
      end
      @args = args
      #AWS S3に接続
      AWS::S3::Base.establish_connection!(@args)
      if create_flg
        begin
          @bucket = AWS::S3::Bucket.find(@bucket_name)
        rescue 
          @bucket = create_bucket
        end
      else
        @bucket = AWS::S3::Bucket.find(@bucket_name)
      end
    end
    def create_bucket()
      S3log.info("Bucket.create(#{@bucket_name})")
      #Bucket作成
      ret = AWS::S3::Bucket.create(@bucket_name) 
      unless ret
        S3log.error("AWS::S3::Bucket create error possibly already exist #{@bucket_name} by anyone else?)")
        exit(-1)
      end
    end
    def rename(orig,dest)
      AWS::S3::S3Object.rename(orig,dest,@bucket_name) 
    end
    def exists?(key)
      key=CGI.escape(key)
      count = 0;
      ret = false
      while true do
        begin
          ret = AWS::S3::S3Object.exists? key,@bucket_name
          break;
        rescue => ex
          count+=1;
          S3log.info("exists? #{count} times failed. #{key}\n")
          if count >= @max_retry_count
            S3log.error("post #{count} times failed #{key_name} #{ex.class}:#{ex.message}\n")
            exit(-1)
          end
          sleep(count*30)
          AWS::S3::Base.establish_connection!(@args)
        end
      end
      return ret;
    end
    def get(key)
      key_name = CGI.escape(key)
      count = 0;
      data = nil
      while true do
        begin
          if AWS::S3::S3Object.exists? key_name,@bucket_name
            S3log.info("AWS::S3::S3Object.value(#{key_name})\n")
            data =  AWS::S3::S3Object.value(key_name,@bucket_name)
          end
          break;
        rescue => ex
          count+=1;
          S3log.info("get #{count} times failed. #{key_name}\n")
          if count >= @max_retry_count
            S3log.error("get  #{count} times failed #{key_name} #{ex.class}:#{ex.message}\n")
            exit(-1)
          end
          sleep(count*30)
          AWS::S3::Base.establish_connection!(@args)
        end
      end
      return data
    end 
    def post(key,val)
      key_name = CGI.escape(key)
      S3log.info("S3Object.store(#{key_name})")
      count = 0;
      while true do
        begin
          AWS::S3::S3Object.store(key_name,val,@bucket_name)
          break;
        rescue => ex
          count+=1;
          S3log.info("post #{count} times failed. #{key_name}\n")
          if count >= @max_retry_count
            S3log.error("post #{count} times failed #{key_name} #{ex.class}:#{ex.message}\n")
            exit(-1)
          end
          sleep(count*30)
          AWS::S3::Base.establish_connection!(@args)
        end
      end
    end
    def set_config(config)
      err_msg = "" 
      unless config["access_key_id"]
        err_msg += "access_key_id doesn't exist in config file.\n"
      end
      @access_key_id = config["access_key_id"]
      unless config["secret_access_key"]
        err_msg += "secret_access_key doesn't exis in config file.\n"
      end
      @secret_access_key = config["secret_access_key"]
      unless config["bucket"]
        err_msg += "bucket doesn't exist in config file.\n"
      end
      if config["max_retry_count"]
        if config["max_retry_count"].class == String
          @max_retry_count = config["max_retry_count"].to_i
        else
          @max_retry_count = config["max_retry_count"]
        end
      else
        @max_retry_count = DEFAULT_MAX_RETRY_COUNT
      end
      @proxy = {}
      @proxy[:host] = config["proxy_host"] if config["proxy_host"]
      @proxy[:port] = config["proxy_port"] if config["proxy_port"]
      @proxy[:user] = config["proxy_user"] if config["proxy_user"]
      @proxy[:password] = config["proxy_password"] if config["proxy_password"]
      @bucket_name = config["bucket"]
      if err_msg != ""
        S3log.error(err_msg)
        exit(-1)
      end
    end
    def delete(key)
      if exists? key
        S3log.info("S3Object.delete(#{CGI.escape(key)})")
        count = 0;
        while true do
          begin
            AWS::S3::S3Object.delete(CGI.escape(key),@bucket_name)
            break
          rescue => ex
            count+=1;
            S3log.info("delete #{count} times failed. #{key}\n")
            if count >= @max_retry_count
              S3log.error("delete #{count} times failed #{key} #{ex.class}:#{ex.message}\n")
              exit(-1)
            end
            sleep(count*30)
            AWS::S3::Base.establish_connection!(@args)
          end
        end
        return true
      end
      return false
    end
    def find(dest)
      @s3objects = @backet.objects unless @s3objects
      find_obj=[]
      keys=@s3objects.map { |o| CGI.unescape(CGI.unescape(o.key.sub("#{@bucket_name}/","")))}
      keys.each do |key|
        if dest.class == Regexp
          if key =~ dest
            find_obj.push(key)
          end
        else
          if key == dest
            find_obj.push(key)
          end
        end
      end
      return find_obj
    end
  end
end
