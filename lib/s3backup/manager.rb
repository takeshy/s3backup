require 'cgi'
require 'time'
require 'fileutils'
require 's3backup/s3log'
require 's3backup/tree_info'
require 's3backup/crypt'
module S3backup
  class Manager
    DEFAULT_BUF_READ_SIZE=1024*1024*32
    def shell_name(str)
      str.gsub!(/[!#"$&'()*,:;<=>?\[\]\\^`{|}\s]/, '\\\\\&')
      a=[]
      str.each_byte{|i| 
        if i < 0x80 
          a.push(sprintf("%c",i)) 
        else 
          a.push("'"+sprintf("%c",i) + "'") 
        end
      }
      return a.join;
    end
    def initialize(target,config)
      @target = target
      @resume = false
      @temporary = "/tmp"
      set_config(config)
    end
    def set_config(config)
      if config["password"] and config["password"] != ""
        unless config["salt"] 
          S3log.error("salt doesn't exist in config file.\n")
          exit(-1)
        end
        unless config["salt"] =~ /[0-9A-Fa-f]{16}/
          S3log.error("salt format shoud be HexString and length should be 16.\n")
          exit(-1)
        end
        @aes = Crypt.new(config["password"],config["salt"])
      end
      if config["buffer_size"]
        if config["buffer_size"].class == String
          @buf_size = config["buffer_size"].to_i
        else
          @buf_size = config["buffer_size"]
        end
        if @buf_size > 1000*1000*1000*5
          S3log.error("buffer_size must be less than 5G\n")
          exit(-1)
        end
      else
        @buf_size = DEFAULT_BUF_READ_SIZE
      end
      if config["temporary"]
        @temporary = config["temporary"]
      end
      if config["resume"] == true
        @resume = true
      end
    end
    def to_gz(file,remain=false)
      if remain
        cmd = "(cd #{shell_name(File.dirname(file))};gzip -c  #{shell_name(file)} >  #{shell_name(file)}.gz " + 
                                              "2>/dev/null)"
      else
        cmd = "(cd #{shell_name(File.dirname(file))};gzip #{shell_name(file)} > /dev/null 2>&1)"
      end
      S3log.debug(cmd)
      system(cmd) 
      unless $?.success?
        S3log.error("feiled #{cmd} execute. #{$?.inspect}")
        exit(-1)
      end
    end
    def from_gz(file)
      cmd = "(cd #{shell_name(File.dirname(file))};gunzip #{shell_name(file)} > /dev/null 2>&1)"
      S3log.debug(cmd)
      system(cmd) 
      unless $?.success?
        S3log.error("feiled #{cmd} execute. #{$?.inspect}")
        exit(-1)
      end
    end
    #指定されたディレクトリをtar gzip形式で圧縮する
    def to_tgz(path,dir)
      #サブディレクトリを圧縮の対象外にする。
      sub_dir = []
      Dir.foreach(dir) do |file|
        next if /^\.+$/ =~ file 
        sub_dir.push(file) if File.directory?(dir+"/"+file)
      end
      exclude = ""
      if sub_dir.length != 0
        exclude = " --exclude=#{shell_name(File.basename(dir))}/" + sub_dir.map{|d| shell_name(d)}.join(
                          "  --exclude=#{shell_name(File.basename(dir))}/") 
      end
      cmd = "(cd #{shell_name(File.dirname(dir))};tar -czvf #{shell_name(path)} #{exclude} -- " + 
        "#{shell_name(File.basename(dir))} > /dev/null 2>&1)"
      S3log.info(cmd)
      system(cmd) 
      unless $?.success?
        S3log.error("feiled #{cmd} execute. #{$?.inspect}")
        exit(-1)
      end
    end
    def from_tgz(path,dir)
      cmd = "tar -xzvf #{shell_name(path)} -C #{shell_name(dir)} > /dev/null 2>&1"
      S3log.info(cmd)
      system(cmd) 
      unless $?.success?
        S3log.error("feiled #{cmd} execute. #{$?.inspect}")
        exit(-1)
      end
    end
    def get_chain(key,path)
      data = nil
      i=1
      if @aes
        key = @aes.encrypt(key)
      end
      File.open(path,"w") do |f|
        while 1
          key_name = i.to_s()+"_"+key
          data = @target.get(key_name)
          if data == nil
            break
          end
          if @aes
            data = @aes.decrypt(data)
          end
          f.write(data)
          i+=1
        end
      end
    end
    def get_directory(dir,out_dir)
      file_name = @temporary + "/rs_#{Process.pid}.tgz"
      get_chain(dir,file_name)
      #tgzのファイルをcur_dirに展開
      from_tgz(file_name,out_dir)
      File.unlink(file_name)
    end
    def get_directories(dirs,prefix,output_dir)
      prefix_len = prefix.length
      dirs.each do |dir|
        parent = File.dirname(dir)
        p_len = parent.length
        relative_path = parent.slice(prefix_len,p_len - prefix_len)
        cur_dir = output_dir + relative_path
        get_directory(dir,cur_dir)
      end
    end
    def store_directory(dir)
      tmp_file = @temporary + "/bk_#{Process.pid}"
      #tgzのファイルをtmp.pathに作成
      to_tgz(tmp_file,dir)
      #S3にディレクトリの絶対パスをキーにして、圧縮したデータをストア
      i=1
      key = nil
      if @aes
        key = @aes.encrypt(dir)
      else
        key = dir
      end
      #前回のバックアップデータ削除
      cnt = 1
      while @target.exists?(cnt.to_s() + "_" + key)
        @target.delete(cnt.to_s() + "_" + key)
        cnt+=1
      end
      File.open(tmp_file,"r") do |f|
        begin
          while 1
            key_name = i.to_s()+"_"+key
            data = f.readpartial(@buf_size)
            if @aes
              data = @aes.encrypt(data)
            end
            @target.post(key_name,data)
            i+=1
          end
        rescue EOFError
        end
      end
      File.unlink(tmp_file)
    end
    def delete_direcory(dir)
      if @aes
        dir = @aes.encrypt(dir)
      end
      i=1
      while @target.delete("#{i}_#{dir}")
        i+=1
      end
    end
    def differential_copy(dir)
      #現在のファイル・ツリーを比較
      tree_info = TreeInfo.new({:format=>:directory,:directory=>dir,:db=>@temporary + "/new_" + 
                               Time.now.to_i.to_s + "_" + Process.pid.to_s + ".db"})
      target_db_name = dir+".gz"
      #前回のファイル・ツリーを取得
      data = @target.get(target_db_name)
      old_tree = nil
      if data
        db_name = @temporary + "/old_" + Time.now.to_i.to_s + "_" + Process.pid.to_s + ".db"
        File.open(db_name + ".gz","w") do |f|
          f.write(data)
        end
        from_gz(db_name + ".gz")
        old_tree = TreeInfo.new({:format=>:database,:db=>db_name})
      else
        target_tree_name = "tree_"+dir+".yml"
        #以前のフォーマットだった場合は変換
        data = @target.get(target_tree_name)
        if data
          old_tree = TreeInfo.new({:format=>:yaml,:data=>data,:db=>@temporary + "/old_" +
                                  Time.now.to_i.to_s + "_" + Process.pid.to_s + ".db"})
        else
          old_tree = TreeInfo.new({:db=>@temporary + "/old_" +
                                  Time.now.to_i.to_s + "_" + Process.pid.to_s + ".db"})
        end
      end
      data = nil;
      GC.start
      cnt=0
      #前回と今回のファイル・ツリーを比較
      tree_info.modify(old_tree) do |dir_info|
        cnt+=1
        S3log.debug("diff_info=#{dir_info[:name]}")
        #更新されたディレクトリをアップロード
        store_directory(dir_info[:name])
        #前回のファイル・ツリー情報のうち、今回アップデートしたディレクトリ情報ファイル情報を更新
        old_dir_map = old_tree.update_dir(dir_info)
        if cnt != 0 and cnt % 10 == 0
          #更新したファイル・ツリー情報をアップロード(途中で失敗しても、resumeできるようにするため。)
          to_gz(old_tree.db_name,true)
          @target.post(target_db_name,File.read(old_tree.db_name + ".gz"))
        end
      end
      tree_info.remove(old_tree) do |dir_info|
        delete_direcory(dir_info[:name])
      end
      #今回のファイル・ツリーをAWS S3に登録
      to_gz(tree_info.db_name)
      @target.post(target_db_name,File.read(tree_info.db_name + ".gz"))
      tree_info.close(true)
      old_tree.close(true)
    end
    def get_target_tree(dir)
      base_dir = dir
      tree_data = nil
      before_base=""
      #バックアップしたディレクトリよりも下位のディレクトリが指定されることがあるため
      while 1
        base = base_dir
        if base == before_base
          break
        end
        tree_db_name = base+".gz"
        tree_data = @target.get(tree_db_name)
        if tree_data
          break
        end
        before_base = base
        base_dir = File.dirname(base_dir)
      end
      unless tree_data
        return nil
      end
      db_name = @temporary + "/" + Time.now.to_i.to_s + "_" + Process.pid.to_s + ".db"
      File.open(db_name + ".gz","w") do |f|
        f.write(tree_data)
      end
      from_gz(db_name + ".gz")
      return TreeInfo.new({:format=>:database,:db=>db_name})
    end
    def expand_tree(dir,tree_info,output_dir)
      now = Time.new
      tree = tree_info.hierarchie(dir)
      top = tree[0].keys[0]
      top_dir = File.dirname(top)
      tmp_dir = CGI.escape(top_dir)
      output_dir = output_dir+"/"+tmp_dir
      FileUtils.mkdir_p(output_dir)
      tree.each do |node|
        get_directories(node.keys,top_dir,output_dir)
      end
      top_dir_len = top_dir.length
      (tree.length - 1).downto(0){|n|
        tree[n].each do |k,v|
          dir_len = k.length
          relative_path = k.slice(top_dir_len,dir_len - top_dir_len)
          dir = output_dir + relative_path
          File.utime(now,Time.parse(v[:mtime]),dir)
        end
      }
    end
    def restore(dir,output_dir)
      tree = get_target_tree(dir)
      unless tree
        S3log.warn("#{dir} isn't find in AWS S3. ignore")
        return
      end
      expand_tree(dir,tree,output_dir)
      tree.close(true)
    end
  end
end
