require 'dmtest/utils'
require 'dmtest/log'
require 'dmtest/fs'

module FIO
  class Task
    def initialize(dev, outfile, scenario)
      @dev = dev
      @outfile = outfile
      @scenario = scenario
    end

    def run
      fs = FS::file_system(:ext4, @dev)
      fs.format(:discard => true)
      fs.with_mount('./fio_test', :discard => false) do
	Dir.chdir('./fio_test') do
	  cfgfile = Tempfile.new('fio-job', '/tmp/')
	  begin
	    if @scenario.has_key?('global')
	      write_job(cfgfile, 'global', @scenario['global'])
	    end

	    @scenario.each do |n, j|
	      next if n == 'global'
	      write_job(cfgfile, n, j)
	    end
	  ensure
	    cfgfile.close
	  end
	  ProcessControl.run("fio #{cfgfile.path} --output=#{@outfile} --minimal")
	  #ProcessControl.run("fio #{cfgfile.path} --output=#{@outfile}")
	end
      end

      parse_terse_file(@outfile)
    end

    private
    def write_job(f, name, vars)
      f.puts "[#{name}]"
      vars.each do |k, v|
	if v == true
	  f.puts k.to_s
	else
	  f.puts "#{k}=#{v}"
	end
      end
    end

    def shift_fields(ary, *keys)
      keys.zip(ary.shift(keys.size)).reduce(Hash.new) do |h, p|
	h[p[0][0]] = p[1].send(p[0][1])
	h
      end
    end

    def shift_latency(ary)
      shift_fields(ary,
		   [:min, :to_i],
		   [:max, :to_i],
		   [:mean, :to_f],
		   [:std_deviation, :to_f])
    end

    def shift_percentiles(ary)
      ary.shift(20).map do |pair|
	p, u = pair.split('%=')
	[p.to_f, u.to_i]
      end.select {|p, u| p >= 0.01}
    end

    # FIXME: parse one line per job
    def parse_terse_line(output)
      # output version 3;fio-2.1.10;randrw;0;0;22214848;370228;5784;60003;4;82;10.173658;7.170526;106;186134;1878.719041;4073.256789;1.000000%=153;5.000000%=233;10.000000%=290;20.000000%=354;30.000000%=446;40.000000%=708;50.000000%=1304;60.000000%=1992;70.000000%=2480;80.000000%=2832;90.000000%=3024;95.000000%=3152;99.000000%=13632;99.500000%=25984;99.900000%=62720;99.950000%=79360;99.990000%=111104;0%=0;0%=0;0%=0;112;186147;1889.001876;4073.944297;219665;551168;100.000000%;375769.586538;77961.946454;22189504;369806;5778;60003;3;83;11.221977;7.519932;42;564993;860.431172;7586.492872;1.000000%=60;5.000000%=62;10.000000%=64;20.000000%=67;30.000000%=70;40.000000%=74;50.000000%=79;60.000000%=83;70.000000%=89;80.000000%=98;90.000000%=114;95.000000%=157;99.000000%=21120;99.500000%=44800;99.900000%=111104;99.950000%=142336;99.990000%=246784;0%=0;0%=0;0%=0;58;564997;871.771213;7587.136854;219537;555264;100.000000%;377090.692308;78801.715763;3.259891%;14.224526%;510582;0;275;0.1%;0.1%;0.1%;0.1%;99.9%;0.0%;0.0%;0.00%;0.00%;0.00%;0.00%;0.01%;40.64%;10.20%;14.04%;3.42%;2.58%;7.30%;19.36%;1.02%;0.59%;0.56%;0.23%;0.07%;0.01%;0.01%;0.00%;0.00%;0.00%
      fields = output.split(';')

      raise "unsupported fio format" unless fields.shift.to_i == 3

      r = {}
      r[:fio_version] = fields.shift
      r[:job_name] = fields.shift
      r[:groupid] = fields.shift
      r[:error] = fields.shift

      r[:total_io_k] = fields.shift.to_i
      r[:bandwidth_kps] = fields.shift.to_i
      r[:iops] = fields.shift.to_i
      r[:runtime_ms] = fields.shift.to_i

      r[:slat_us] = shift_latency(fields)
      r[:clat_us] = shift_latency(fields)
      r[:clat_percentiles] = shift_percentiles(fields)
      r[:lat_us] = shift_latency(fields)
      r[:bandwidth] = shift_fields(fields,
				   [:min, :to_i],
				   [:max, :to_i],
				   [:aggregate_percent_of_total, :to_s],
				   [:mean, :to_f],
				   [:std_deviation, :to_f])
      r[:cpu] = shift_fields(fields,
			     [:user, :to_f],
			     [:system, :to_f],
			     [:context_switches, :to_i],
			     [:major_faults, :to_i],
			     [:minor_faults, :to_i])

      r
    end

    def parse_terse_file(outfile)
      r = []

      File.open(outfile, 'r') do |file|
	file.each_line do |l|
	  r << parse_terse_line(l)
	end
      end

      r
    end
  end
end
