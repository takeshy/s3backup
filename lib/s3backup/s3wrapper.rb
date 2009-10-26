require 'cgi'
require 'aws/s3'
require 's3backup/crypt'
module S3backup
  class S3Wrapper
    attr_reader :bucket
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
      #AWS S3に接続
      AWS::S3::Base.establish_connection!(args)
      if create_flg
        begin
          @bucket = AWS::S3::Bucket.find(@bucket_name)
        rescue AWS::S3::NoSuchBucket
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
        raise "AWS::S3::Bucket create error"
      end
    end
    def rename(orig,dest)
      AWS::S3::S3Object.rename(orig,dest,@bucket_name) 
    end
    def exists?(key)
      key=CGI.escape(key)
      ret = AWS::S3::S3Object.exists? key,@bucket_name
    end
    def get(key)
      key_name = CGI.escape(key)
      data = nil
      if AWS::S3::S3Object.exists? key_name,@bucket_name
        data =  AWS::S3::S3Object.value(key_name,@bucket_name)
      end
      return data
    end 
    def post(key,val)
      key_name = CGI.escape(key)
      S3log.info("S3Object.store(#{key_name})")
      AWS::S3::S3Object.store(key_name,val,@bucket_name)
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
      @proxy = {}
      @proxy[:host] = config["proxy_host"] if config["proxy_host"]
      @proxy[:port] = config["proxy_port"] if config["proxy_port"]
      @proxy[:user] = config["proxy_user"] if config["proxy_user"]
      @proxy[:password] = config["proxy_password"] if config["proxy_password"]
      @bucket_name = config["bucket"]
      if err_msg != ""
        raise err_msg
      end
    end
    def delete(key)
      if exists? key
        S3log.info("S3Object.delete(#{CGI.escape(key)})")
        AWS::S3::S3Object.delete(CGI.escape(key),@bucket_name)
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
