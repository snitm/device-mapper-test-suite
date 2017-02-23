require 'dmtest/log'
require 'dmtest/process'
require 'dmtest/utils'
require 'dmtest/disk-units'
require 'dmtest/thinp-test'
require 'timeout'
require 'rspec/expectations'

#----------------------------------------------------------------

class PoolAndDataSizeMatchTests < ThinpTestCase
  include Utils
  include TinyVolumeManager
  extend TestUtils

  def setup
    super
    @low_water_mark = 0
    @data_block_size = 128

    @tvm = VM.new
    @tvm.add_allocation_volume(@data_dev)

    @size = @volume_size
    @tvm.add_volume(linear_vol('data', @size))

    wipe_device(@metadata_dev, 8)
  end

  def pool_table(size, data)
    Table.new(ThinPoolTarget.new(size, @metadata_dev, data,
                                 @data_block_size, @low_water_mark))
  end

  #--------------------------------

  tag :thinp_target

  define_test :data_cannot_be_smaller_than_pool_on_initial_load do
    with_dev(@tvm.table('data')) do |data|
      table = pool_table(@size * 2, data)

      begin
        with_dev(table) {}
      rescue
        failed = true
      end

      failed.should be_true
    end
  end

  define_test :data_cannot_be_smaller_than_pool_on_reload do
    with_dev(@tvm.table('data')) do |data|
      table = pool_table(@size, data)
      table2 = pool_table(@size * 2, data)

      with_dev(table) do |pool|
        failed = false

        begin
          pool.pause do
            pool.load(table2)
          end
        rescue
          failed = true
        end

        failed.should be_true
        status = PoolStatus.new(pool)
        status.total_data_blocks.should == @size / @data_block_size
      end
    end
  end

  define_test :extra_data_space_must_not_be_used_on_initial_load do
    with_dev(@tvm.table('data')) do |data|
      table = pool_table(@size / 2, data)

      with_dev(table) do |pool|
        status = PoolStatus.new(pool)
        status.total_data_blocks.should == @size / 2 / @data_block_size
      end
    end
  end

  define_test :extra_data_space_must_not_be_used_on_suspend do
    with_dev(@tvm.table('data')) do |data|
      table = pool_table(@size / 2, data)

      with_dev(table) do |pool|
        @tvm.resize('data', @size)

        pool.pause do
          data.pause do
            data.load(@tvm.table('data'))
          end
        end

        status = PoolStatus.new(pool)
        status.total_data_blocks.should == @size / 2 / @data_block_size
      end
    end
  end

  define_test :extra_data_space_must_not_be_used_on_reload do
    with_dev(@tvm.table('data')) do |data|
      table = pool_table(@size / 2, data)

      with_dev(table) do |pool|
        @tvm.resize('data', @size)

        pool.pause do
          data.pause do
            data.load(@tvm.table('data'))
          end

          pool.load(table)
        end

        status = PoolStatus.new(pool)
        status.total_data_blocks.should == @size / 2 / @data_block_size
      end
    end
  end
end

#----------------------------------------------------------------

class PoolReloadWithSpaceTests < ThinpTestCase
  include Utils
  include TinyVolumeManager
  extend TestUtils

  def setup
    super
    @low_water_mark = 0
    @data_block_size = 128
  end

  #--------------------------------

  tag :thinp_target

  define_test :reload_no_io do
    table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, @data_dev,
                                         @data_block_size, @low_water_mark))

    with_dev(table) do |pool|
      pool.pause do
        pool.load(table)
      end
    end
  end

  define_test :reload_io do
    table = Table.new(ThinPoolTarget.new(20971520, @metadata_dev, @data_dev,
                                         @data_block_size, @low_water_mark))

    with_dev(table) do |pool|
      with_new_thin(pool, @volume_size, 0) do |thin|
        tid = Thread.new(thin) do |thin|
          wipe_device(thin)
        end

        ProcessControl.sleep 5

        # All thins must be paused before reloading the pool
        thin.pause do
          pool.pause do
            pool.load(table)
          end
        end

        tid.join
      end
    end
  end
end

#----------------------------------------------------------------

class PoolResizeWithSpaceTests < ThinpTestCase
  include Utils
  include TinyVolumeManager
  include DiskUnits
  extend TestUtils

  def setup
    super
    @low_water_mark = 0
    @data_block_size = 128
  end

  def pool_table(size)
    Table.new(ThinPoolTarget.new(size, @metadata_dev, @data_dev,
                                 @data_block_size, @low_water_mark))
  end

  #--------------------------------

  tag :thinp_target

  define_test :resize do
    target_step = @size / 8
    with_standard_pool(target_step) do |pool|
      2.upto(8) do |n|
        table = pool_table(n * target_step)
        pool.pause do
          pool.load(table)
        end
      end
    end
  end

  define_test :commit_failure_sets_needs_check do
    tvm = VM.new
    tvm.add_allocation_volume(@metadata_dev)
    tvm.add_volume(linear_vol('metadata', dev_size(@metadata_dev)))

    volume_size = gig(3)

    with_dev(tvm.table('metadata')) do |metadata|
      wipe_device(metadata, 8)

      # use higher low water mark, wait for it to trigger, establish flakey target for metadata
      low_water_mark = (volume_size / 3) / @data_block_size
      table = Table.new(ThinPoolTarget.new(volume_size, metadata, @data_dev,
                                           @data_block_size, low_water_mark))
      with_dev(table) do |pool|
        with_new_thin(pool, volume_size, 0) do |thin|
          event_tracker = pool.event_tracker;

          fork {
            begin
              wipe_device(thin)
            rescue
            end
          }

          event_tracker.wait do
            status = PoolStatus.new(pool)
            status.used_data_blocks >= status.total_data_blocks - low_water_mark
          end

          thin.pause_noflush do
            pool.pause do
              # Establish flakey metadata.  We need the superblock to remain working.
              superblock_size = 8
              table = Table.new(LinearTarget.new(superblock_size, @metadata_dev, 0),
                                FlakeyTarget.new(dev_size(@metadata_dev) - superblock_size,
                                                 @metadata_dev, superblock_size, 0, 60, false, true))
              metadata.pause do
                metadata.load(table)
              end
            end
          end

          assert(read_only_or_fail_mode?(pool))
        end

        # load identical table, should result in error about inability
        # to switch pool to write mode due to 'needs_check'.
        status = PoolStatus.new(pool)
        status.needs_check.should be_true

        table = pool.active_table
        pool.pause do
          pool.load(table)
        end
      end
    end
  end
end

#----------------------------------------------------------------

class PoolResizeWhenOutOfSpaceTests < ThinpTestCase
  include Utils
  include TinyVolumeManager
  include DiskUnits
  extend TestUtils

  def setup
    super
    @low_water_mark = 0
    @data_block_size = 128
  end

  def in_out_of_data_mode(pool)
      status = PoolStatus.new(pool)
      status.options[:mode] == :out_of_data_space
  end

  def wait_until_out_of_data(pool)
    pool.event_tracker.wait do
      in_out_of_data_mode(pool)
    end
  end

  def wipe_expecting_error(dev)
    failed = false
    begin
      wipe_device(dev)
    rescue
      failed = true
    end

    failed.should be_true
  end

  #--------------------------------

  tag :thinp_target

  define_test :out_of_data_space_errors_immediately_if_requested do
    with_standard_pool(@volume_size / 2, :error_if_no_space => true) do |pool|
      failed = false

      with_new_thin(pool, @volume_size, 0) do |thin|
        begin
          wipe_device(thin)
        rescue
          failed = true
        end
      end

      failed.should be_true
      status = PoolStatus.new(pool)
      status.options[:mode].should == :out_of_data_space
    end
  end

  define_test :out_of_data_space_times_out do
    with_standard_pool(@volume_size / 2, :error_if_no_space => false) do |pool|
      failed = false

      with_new_thin(pool, @volume_size, 0) do |thin|
        begin
          wipe_device(thin)
        rescue
          failed = true
        end
      end

      failed.should be_true
      status = PoolStatus.new(pool)
      status.options[:mode].should == :out_of_data_space
      status.needs_check.should be_false
    end
  end

  define_test :resize_after_OODS_error_immediately do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark,
                                           true, true, true, false, true))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          wipe_expecting_error(thin)

          thin.pause_noflush do
            # new size of the pool/data device
            @size *= 4

            # resize the underlying data device
            tvm.resize('data', @size)
            data.pause do
              data.load(tvm.table('data'))
            end

            # resize the pool
            pool.pause do
              table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                    @data_block_size, @low_water_mark,
                                                    true, true, true, false, true))
              pool.load(table2)
            end

            status = PoolStatus.new(pool)
            status.options[:mode].should == :read_write
          end
        end
      end
    end
  end

  define_test :resize_after_OODS_held_io do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          tid = Thread.new(thin) do |thin|
            # If this errors then the exception _will_ be reported
            wipe_device(thin)
          end

          wait_until_out_of_data(pool)

          thin.pause_noflush do
            # new size of the pool/data device
            @size *= 4

            in_out_of_data_mode(pool).should be_true

            # resize the underlying data device
            tvm.resize('data', @size)
            data.pause do
              data.load(tvm.table('data'))
            end

            # resize the pool
            pool.pause do
              table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                    @data_block_size, @low_water_mark))
              pool.load(table2)
            end
          end

          tid.join
        end
      end
    end
  end

  #--------------------------------
  # LVM loads a table before the suspend/resume cycle.  So we need to
  # test preload scenarios.

  define_test :resize_after_OODS_held_io_preload do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          tid = Thread.new(thin) do |thin|
            # If this errors then the exception _will_ be reported
            wipe_device(thin)
          end

          wait_until_out_of_data(pool)
          in_out_of_data_mode(pool).should be_true

          # Preload the underlying data device
          @size *= 4
          tvm.resize('data', @size)
          data.load(tvm.table('data'))

          # switch to new data dev table
          data.resume

          # preload the pool
          table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                @data_block_size, @low_water_mark))
          pool.load(table2)

          # switch to new pool table
          pool.resume

          tid.join
        end
      end
    end
  end

  define_test :resize_after_OODS_held_io_timed_out_preload do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          begin
            wipe_device(thin)
          rescue
            failed = true
          end

          failed.should be_true
          status = PoolStatus.new(pool)
          status.options[:mode].should == :out_of_data_space
          status.options[:error_if_no_space].should be_true

          # Preload the underlying data device
          @size *= 4
          tvm.resize('data', @size)
          data.load(tvm.table('data'))

          # switch to new data dev table
          data.resume

          # preload the pool
          table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                @data_block_size, @low_water_mark))
          pool.load(table2)

          # switch to new pool table
          pool.resume

          wipe_device(thin)
        end
      end
    end
  end

  # bz1184592
  def _test_thin_create_after_OOD
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          begin
            wipe_device(thin)
          rescue
            failed = true
          end

          failed.should be_true
          status = PoolStatus.new(pool)
          status.options[:mode].should == :out_of_data_space

          # Create a thin, this should fail (FIXME: no longer the case)
          begin
            with_new_thin(pool, @volume_size, 1) do |thin2|
            end
            create_failed = false
          rescue
            create_failed = true
          end

          # FIXME: create doesn't fail if in OODS
          create_failed.should be_false

          # Preload the underlying data device
          @size *= 4
          tvm.resize('data', @size)
          data.load(tvm.table('data'))

          # switch to new data dev table
          data.resume

          # preload the pool
          table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                @data_block_size, @low_water_mark))
          pool.load(table2)

          # switch to new pool table
          pool.resume

          # retry the create, which should now succeed
          with_new_thin(pool, @volume_size, 1) do |thin2|
            wipe_device(thin2)
          end
        end
      end
    end

  end

  # bz #1095639
  define_test :io_to_provisioned_region_with_OODS_held_io do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|
          tid = Thread.new(thin) do |thin|
            # If this errors then the exception _will_ be reported
            wipe_device(thin)
          end

          wait_until_out_of_data(pool)

          ProcessControl.run("dd iflag=direct if=#{thin.path} of=/dev/null bs=4194304 count=2")
          ProcessControl.run("dd oflag=direct of=#{thin.path} if=/dev/zero bs=4194304 count=2")

          thin.pause_noflush do
            # new size of the pool/data device
            @size *= 4

            in_out_of_data_mode(pool).should be_true

            # resize the underlying data device
            tvm.resize('data', @size)
            data.pause do
              data.load(tvm.table('data'))
            end

            # resize the pool
            pool.pause do
              table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                    @data_block_size, @low_water_mark))
              pool.load(table2)
            end
          end

          tid.join
        end
      end
    end
  end

  #--------------------------------

  # https://bugzilla.redhat.com/show_bug.cgi?id=1091852
  #
  # This test isn't great; it only intermittently failed before the
  # bug was fixed.
  define_test :resize_after_OODS_held_io_ext4 do
    tvm = VM.new
    tvm.add_allocation_volume(@data_dev)

    @size = @volume_size / 2
    tvm.add_volume(linear_vol('data', @volume_size / 2))

    with_dev(tvm.table('data')) do |data|
      wipe_device(@metadata_dev, 8)

      table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                           @data_block_size, @low_water_mark))

      with_dev(table) do |pool|
        with_new_thin(pool, @volume_size, 0) do |thin|

          # We run the resize operation in a separate thread.  A bit
          # like dmeventd.
          tid = Thread.new(thin) do |thin|
            wait_until_out_of_data(pool)
            sleep 5            # sleep to allow ext4 a good sized window to try and write to the journal

            thin.pause_noflush do
              # new size of the pool/data device
              @size *= 4

              in_out_of_data_mode(pool).should be_true

              # resize the underlying data device
              tvm.resize('data', @size)
              data.pause do
                data.load(tvm.table('data'))
              end

              # resize the pool
              pool.pause do
                table2 = Table.new(ThinPoolTarget.new(@size, @metadata_dev, data,
                                                      @data_block_size, @low_water_mark))
                pool.load(table2)
              end
            end
          end

          fs = FS::file_system(:ext4, thin)
          fs.format

          fs.with_mount('./bench_mnt') do
            Dir.chdir('./bench_mnt') do
              write_size = @volume_size - meg(256) # take off a bit for the fs overhead
              block_size = meg(1)
              count = write_size / block_size
              block_size *= 512 # convert to bytes
              ProcessControl.run("dd if=/dev/zero of=./big_file oflag=direct bs=#{block_size} count=#{count}")
            end
          end

          tid.join
        end
      end
    end
  end

  def resize_io_many(n)
    target_step = round_up(@volume_size / n, @data_block_size)
    with_standard_pool(target_step) do |pool|
      with_new_thin(pool, @volume_size, 0) do |thin|
        event_tracker = pool.event_tracker;

        fork {dt_device(thin, "sequential")}

        2.upto(n) do |i|
          wait_until_out_of_data(pool)

          in_out_of_data_mode(pool).should be_true

          thin.pause_noflush do
            pool.pause do
              table = Table.new(ThinPoolTarget.new(i * target_step, @metadata_dev, @data_dev,
                                                   @data_block_size, @low_water_mark))
              pool.load(table)
            end
          end
        end

        Process.wait
        if $?.exitstatus > 0
          raise RuntimeError, "wipe sub process failed"
        end
      end

      # suspend/resume cycle should _not_ cause read-write -> read-only!
      pool.pause {}

      status = PoolStatus.new(pool)
      status.options[:mode].should == :read_write

      # remove the created thin
      pool.message(0, 'delete 0')
    end
  end

  define_test :resize_io do
    resize_io_many(8)
  end

  # see BZ #769921
  def _test_ext4_runs_out_of_space
    # we create a pool with a really tiny data volume that wont be
    # able to complete a mkfs.
    with_standard_pool(16) do |pool|
      with_new_thin(pool, @volume_size, 0) do |thin|

        event_tracker = pool.event_tracker;

        thin_fs = FS::file_system(:ext4, thin)
        fork {thin_fs.format}

        # FIXME: this is so common it should go into a utility lib
        event_tracker.wait do
          status = PoolStatus.new(pool)
          status.used_data_blocks >= status.total_data_blocks - @low_water_mark
        end

        # we're not sure what the development version of dmeventd was
        # doing to create the issue; some experiments to find out.
        pool.info

        # Resize the pool so the format can complete
        table = Table.new(ThinPoolTarget.new(@size, @metadata_dev, @data_dev,
                                             @data_block_size, @low_water_mark))
        pool.load(table)
        pool.resume
      end
    end

    Process.wait
    if $?.exitstatus > 0
      raise RuntimeError, "wipe sub process failed"
    end
  end
end

#----------------------------------------------------------------
