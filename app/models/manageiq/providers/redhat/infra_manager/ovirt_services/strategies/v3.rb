module ManageIQ::Providers::Redhat::InfraManager::OvirtServices::Strategies
  class V3
    attr_reader :ext_management_system

    def initialize(args)
      @ext_management_system = args[:ems]
    end

    def get
      self
    end

    # Event parsing

    def username_by_href(href)
      ext_management_system.with_provider_connection do |rhevm|
        Ovirt::User.find_by_href(rhevm, href).try(:[], :user_name)
      end
    end

    def cluster_name_href(href)
      ext_management_system.with_provider_connection do |rhevm|
        Ovirt::Cluster.find_by_href(rhevm, href).try(:[], :name)
      end
    end

    # Provisioning
    def get_host_proxy(host, connection)
      connection ||= ext_management_system.connect
      host_proxy = connection.get_resource_by_ems_ref(host.ems_ref)
      GeneralUpdateMethodNamesDecorator.new(host_proxy)
    end

    def clone_completed?(args)
      source = args[:source]
      phase_context = args[:phase_context]
      logger = args[:logger]
      # TODO: shouldn't this error out the provision???
      return true if phase_context[:clone_task_ref].nil?

      source.with_provider_connection do |rhevm|
        status = rhevm.status(phase_context[:clone_task_ref])
        logger.info("Clone is #{status}")
        status == 'complete'
      end
    end

    def destination_image_locked?(vm)
      vm.with_provider_object do |rhevm_vm|
        return false if rhevm_vm.nil?
        rhevm_vm.attributes.fetch_path(:status, :state) == "image_locked"
      end
    end

    def populate_phase_context(phase_context, vm)
      phase_context[:new_vm_ems_ref] = ManageIQ::Providers::Redhat::InfraManager.make_ems_ref(vm[:href])
      phase_context[:clone_task_ref] = vm.creation_status_link
    end

    def nics_for_(vm)
      vm.with_provider_object do |rhevm_vm|
        rhevm_vm.nics.collect { |n| NicsDecorator.new(n) }
      end
    end

    def cluster_find_network_by_name(href, network_name)
      ext_management_system.with_provider_connection do |rhevm|
        Ovirt::Cluster.find_by_href(rhevm, href).try(:find_network_by_name, network_name)
      end
    end

    def configure_vnic(args)
      vm = args[:vm]
      mac_addr = args[:mac_addr]
      network = args[:network]
      nic_name = args[:nic_name]
      interface = args[:interface]
      vnic = args[:vnic]
      logger = args[:logger]

      options = {
        :name        => nic_name,
        :interface   => interface,
        :network_id  => network[:id],
        :mac_address => mac_addr,
      }.delete_blanks

      logger.info("with options: <#{options.inspect}>")

      if vnic.nil?
        vm.with_provider_object do |rhevm_vm|
          rhevm_vm.create_nic(options)
        end
      else
        vnic.apply_options!(options)
      end
    end

    def powered_off_in_provider?(vm)
      vm.with_provider_object(&:status)[:state] == "down"
    end

    def powered_on_in_provider?(vm)
      vm.with_provider_object(&:status)[:state] == "up"
    end

    def vm_boot_from_cdrom(operation, name)
      begin
        operation.get_provider_destination.boot_from_cdrom(name)
      rescue Ovirt::VmNotReadyToBoot
        raise OvirtServices::VmNotReadyToBoot
      end
    end

    def vm_boot_from_network(operation)
      begin
        operation.get_provider_destination.boot_from_network
      rescue Ovirt::VmNotReadyToBoot
        raise OvirtServices::VmNotReadyToBoot
      end
    end

    def get_template_proxy(template, connection)
      connection ||= ext_management_system.connect
      template_proxy = connection.get_resource_by_ems_ref(template.ems_ref)
      GeneralUpdateMethodNamesDecorator.new(template_proxy)
    end

    def get_vm_proxy(vm, connection)
      connection ||= ext_management_system.connect
      vm_proxy = connection.get_resource_by_ems_ref(vm.ems_ref)
      GeneralUpdateMethodNamesDecorator.new(vm_proxy)
    end

    def collect_disks_by_hrefs(disks)
      vm_disks = []

      ext_management_system.try(:with_provider_connection) do |rhevm|
        disks.each do |disk|
          begin
            vm_disks << Ovirt::Disk.find_by_href(rhevm, disk)
          rescue Ovirt::MissingResourceError
            nil
          end
        end
      end
      vm_disks
    end

    def shutdown_guest(operation)
      operation.with_provider_object(&:shutdown)
      rescue Ovirt::VmIsNotRunning
    end

    def start_clone(source, clone_options, phase_context)
      source.with_provider_object do |rhevm_template|
        vm = rhevm_template.create_vm(clone_options)
        populate_phase_context(phase_context, vm)
      end
    end

    def vm_start(vm, cloud_init)
      vm.with_provider_object do |rhevm_vm|
        rhevm_vm.start { |action| action.use_cloud_init(true) if cloud_init }
      end
      rescue Ovirt::VmAlreadyRunning
    end

    def vm_stop(vm)
      vm.with_provider_object(&:stop)
      rescue Ovirt::VmIsNotRunning
    end

    def vm_suspend(vm)
      vm.with_provider_object(&:suspend)
    end

    class NicsDecorator < SimpleDelegator
      def name
        self[:name]
      end

      def network
        id = self[:network][:id]
        OpenStruct.new(:id => id)
      end
    end

    class GeneralUpdateMethodNamesDecorator < SimpleDelegator
      def method_missing(method_name, *args)
        str_method_name = method_name.to_s
        if str_method_name =~ /update_.*!/
          attribute_to_update = str_method_name.split("update_")[1].gsub('!','')
          send("#{attribute_to_update}=", *args)
        else
          # This is requied becasue of Ovirt::Vm strage behaviour - while rhevm.respond_to?(:nics)
          # returns false, rhevm.nics actually works.
          begin
            __getobj__.send(method_name, *args)
          rescue NoMethodError
            super
          end
        end
      end
    end
  end
end
