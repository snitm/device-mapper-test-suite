require 'dmtest/blktrace'
require 'dmtest/cache-status'
require 'dmtest/cache_policy'
require 'dmtest/cache_stack'
require 'dmtest/cache_utils'
require 'dmtest/disk-units'
require 'dmtest/git'
require 'dmtest/log'
require 'dmtest/test-utils'
require 'dmtest/thinp-test'
require 'dmtest/tvm.rb'
require 'dmtest/utils'

require 'rspec/expectations'

#----------------------------------------------------------------

class IOUseTests < ThinpTestCase
  include GitExtract
  include Utils
  include BlkTrace
  include DiskUnits
  include CacheUtils
  extend TestUtils

  POLICY_NAMES = %w(mq smq)
  IO_MODES = [:writethrough, :writeback]

  def no_io_when_idle(policy, io_mode)
    s = CacheStack.new(@dm, @metadata_dev, @data_dev,
                       :data_size => gig(2),
                       :block_size => k(64),
                       :cache_size => gig(1),
                       :io_mode => io_mode,
                       :policy => Policy.new(policy))
    s.activate_support_devs do
      s.prepare_populated_cache(:dirty_percentage => 0)
      s.activate_top_level do
        sleep 10                # give udev time to examine the devs

        STDERR.puts "beginning idle period"
        traces, _ = blktrace(@metadata_dev, @data_dev) do
          sleep 30
        end
        STDERR.puts "done"

        assert(traces[0].empty?)
        assert(traces[1].empty?)
      end
    end
  end

  define_tests_across(:no_io_when_idle, POLICY_NAMES, IO_MODES)

  def do_split(dev, fs_type, format_opts = {})
    fs = FS::file_system(fs_type, dev)
    STDERR.puts "formatting ..."
    fs.format(format_opts)

    fs.with_mount('./kernel_builds', :discard => false) do
      Dir.chdir('./kernel_builds') do
        STDERR.puts "creating file ..."
        ProcessControl.run("dd if=/dev/zero of=masterfile bs=1024 count=1000000")
        # FIXME: somehow split is creating over 4G of files!?
        ProcessControl.run("df -h")
        ProcessControl.run("dmsetup table")
        STDERR.puts "splitting file ..."
        ProcessControl.run("split -b 1000 -a 10 masterfile")
        #STDERR.puts "stopping IO to slow data device just before unmount ..."
        #ProcessControl.run("dmsetup message nvme_mpath 0 fail_path /dev/nvme0n1")
      end
    end
  end


  def do_split_large_file(opts)
    stack = CacheStack.new(@dm, @metadata_dev, @data_dev, opts)
    stack.activate do |stack|
      do_split(stack.cache, :xfs)
      STDERR.puts "stopping IO to slow data device just before device teardown ..."
      ProcessControl.run("dmsetup message nvme_mpath_real 0 fail_path /dev/nvme0n1")
    end
  end

  define_test :split_large_file do
    do_split_large_file(:policy => Policy.new('smq'),
                        :block_size => k(64),
                        :metadata_size => meg(40),
                        # need to try with gig(48) cache_size... instead of pmem use nvme partition for fast+meta device?
                        :cache_size => gig(48),
                        # would like to get up to 512GB to match customer but...
                        #:data_size => gig(512),
                        :data_size => gig(210),
                        :io_mode => :writeback,
                        :metadata_version => 2)
  end

end

#----------------------------------------------------------------
