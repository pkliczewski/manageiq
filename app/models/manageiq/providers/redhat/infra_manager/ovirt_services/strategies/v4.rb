module ManageIQ::Providers::Redhat::InfraManager::OvirtServices::Strategies
  class V4
    attr_reader :ext_management_system

    def initialize(args)
      @ext_management_system = args[:ems]
    end

    def username_by_href(href)
      ext_management_system.with_provider_connection(:version => 4) do |connection|
        user = connection.system_service.users_service.user_service(uuid_from_href(href)).get
        "#{user.name}@#{user.domain.name}"
      end
    end

    def cluster_name_href(href)
      ext_management_system.with_provider_connection(:version => 4) do |connection|
        cluster_proxy_from_href(href, connection).name
      end
    end

    # Provisioning
    def get_host_proxy(host, connection)
      connection.system_service.hosts_service.host_service(host.uid_ems)
    end

    def clone_completed?(args)
      source = args[:source]
      phase_context = args[:phase_context]
      logger = args[:logger]

      source.with_provider_connection(:version => 4) do |connection|
        vm = vm_service_by_href(phase_context[:new_vm_ems_ref], connection).get
        status = vm.status
        logger.info("The Vm being cloned is #{status}")
        status == OvirtSDK4::VmStatus::DOWN
      end
    end

    def destination_image_locked?(vm)
      vm.with_provider_object(:version => 4) do |vm_proxy|
        vm_proxy.get.status == OvirtSDK4::VmStatus::IMAGE_LOCKED
      end
    end

    def populate_phase_context(phase_context, vm)
      phase_context[:new_vm_ems_ref] = ManageIQ::Providers::Redhat::InfraManager.make_ems_ref(vm.href)
    end

    def nics_for_(vm)
      vm.with_provider_connection(:version => 4) do |connection|
        vm_proxy = connection.system_service.vms_service.vm_service(vm.uid_ems).get
        connection.follow_link(vm_proxy.nics)
      end
    end

    def cluster_find_network_by_name(href, network_name)
      ext_management_system.with_provider_connection(:version => 4) do |connection|
        cluster_service = connection.system_service.clusters_service.cluster_service(uuid_from_href(href))
        networks = cluster_service.networks_service.list
        networks.detect { |n| n.name == network_name }
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

      vm.with_provider_connection(:version => 4) do |connection|
        uuid = uuid_from_href(vm.ems_ref)
        profile_id = network_profile_id(connection, network.id)
        nics_service = connection.system_service.vms_service.vm_service(uuid).nics_service
        options = {
                    :name         => nic_name || vnic.name,
                    :interface    => interface || vnic.interface,
                    :mac          => mac_addr ? OvirtSDK4::Mac.new({:address => mac_addr}) : vnic.mac,
                    :vnic_profile => profile_id ? { id: profile_id } : vnic.vnic_profile
                  }
        logger.info("with options: <#{options.inspect}>")
        if vnic
          nics_service.nic_service(vnic.id).update(options)
        else
          nics_service.add(OvirtSDK4::Nic.new(options))
        end
      end
    end

    def powered_off_in_provider?(vm)
      vm.with_provider_object(:version => 4) { |vm_service| vm_service.get.status } == OvirtSDK4::VmStatus::DOWN
    end

    def powered_on_in_provider?(vm)
      vm.with_provider_object(:version => 4) { |vm_service| vm_service.get.status } == OvirtSDK4::VmStatus::UP
    end

    def vm_boot_from_cdrom(operation, name)
      begin
        operation.get_provider_destination.vm_service.start(
            vm: {
                os: {
                    boot: {
                        devices: [
                            OvirtSDK4::BootDevice::CDROM
                        ]
                    }
                },
                cdroms: [
                    {
                        id: name
                    }
                ]
            }
        )
      rescue OvirtSDK4::Error
        raise OvirtServices::VmNotReadyToBoot
      end
    end

    def vm_boot_from_network(operation)
      begin
        operation.get_provider_destination.start(vm: {
            os: {
                boot: {
                    devices: [
                        OvirtSDK4::BootDevice::NETWORK
                    ]
                }
            }
        })
      rescue OvirtSDK4::Error
        raise OvirtServices::VmNotReadyToBoot
      end
    end

    def get_template_proxy(template, connection)
      TemplateProxyDecorator.new(
        connection.system_service.templates_service.template_service(template.uid_ems),
        connection,
        self
      )
    end

    def get_vm_proxy(vm, connection)
      VmProxyDecorator.new(connection.system_service.vms_service.vm_service(vm.uid_ems))
    end

    def collect_disks_by_hrefs(disks)
      vm_disks = []
      @ems.with_provider_connection(:version => 4) do |connection|
        disks.each do |disk|
          parts = URI(disk).path.split('/')
          begin
            vm_disks << connection.system_service.storage_domains_service.storage_domain_service(parts[2]).disks_service.disk_service(parts[4]).get
          rescue OvirtSDK4::Error
            nil
          end
        end
      end
      vm_disks
    end

    def shutdown_guest(operation)
      operation.with_provider_object(:version => 4, &:shutdown)
      rescue OvirtSDK4::Error
    end

    def start_clone(source, clone_options, phase_context)
      source.with_provider_object(:version => 4) do |rhevm_template|
        vm = rhevm_template.create_vm(clone_options)
        populate_phase_context(phase_context, vm)
      end
    end

    def vm_start(vm, cloud_init)
      opts = {}
      vm.with_provider_object(:version => 4) do |rhevm_vm|
        opts = {:use_cloud_init => cloud_init} if cloud_init
        rhevm_vm.start(opts)
      end
    rescue OvirtSDK4::Error
    end

    def vm_stop(vm)
      vm.with_provider_object(:version => 4, &:stop)
      rescue OvirtSDK4::Error
    end

    def vm_suspend(vm)
      vm.with_provider_object(:version => 4, &:suspend)
    end

    class VmProxyDecorator < SimpleDelegator
      def update_memory_reserve!(memory_reserve_size)
        vm = get
        vm.memory_policy.guaranteed = memory_reserve_size
        update(vm)
      end

      def update_description!(description)
        vm = get
        vm.description = description
        update(vm)
      end

      def update_memory!(memory)
        vm = get
        vm.memory = memory
        update(vm)
      end

      def update_host_affinity!(dest_host_ems_ref)
        vm = get
        host = collect_host(dest_host_ems_ref)
        vm.placement_policy.hosts = [host]
        update(vm)
      end

      def update_cpu_topology!(cpu_hash)
        vm = get
        vm.cpu.topology = OvirtSDK4::CpuTopology.new(cpu_hash)
        update(vm)
      end
    end

    class TemplateProxyDecorator < SimpleDelegator
      attr_reader :connection, :ovirt_services
      def initialize(template_service, connection, ovirt_services)
        @obj = template_service
        @connection = connection
        @ovirt_services = ovirt_services
        super(template_service)
      end

      def create_vm(options)
        vms_service = connection.system_service.vms_service
        cluster = ovirt_services.cluster_from_href(options[:cluster], connection)
        template = get
        vm = build_vm_from_hash(:name     => options[:name],
                                :template => template,
                                :cluster  => cluster)
        vms_service.add(vm)
      end

      def build_vm_from_hash(args)
        OvirtSDK4::Vm.new(:name => args[:name],
                          :template => args[:template],
                          :cluster => args[:cluster])
      end
    end

    def cluster_from_href(href, connection)
      connection.system_service.clusters_service.cluster_service(uuid_from_href(href)).get
    end

    private

    def cluster_proxy_from_href(href, connection)
      connection.system_service.clusters_service.cluster_service(uuid_from_href(href)).get
    end

    def uuid_from_href(ems_ref)
      URI(ems_ref).path.split('/').last
    end

    def vm_service_by_href(href, connection)
      vm_uuid = uuid_from_href(href)
      connection.system_service.vms_service.vm_service(vm_uuid)
    end

    def network_profile_id(connection, network_id)
      profiles_service = connection.system_service.vnic_profiles_service
      profile = profiles_service.list.detect{ |profile| profile.network.id == network_id }
      profile && profile.id
    end
  end
end
