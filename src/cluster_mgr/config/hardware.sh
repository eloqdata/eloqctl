case "`uname`" in
    Linux)
        system_memory_in_mb=`free -m | awk '/:/ {print $2;exit}'`
        system_cpu_cores=`egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo`
    ;;
    FreeBSD)
        system_memory_in_bytes=`sysctl hw.physmem | awk '{print $2}'`
        system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
        system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
    ;;
    SunOS)
        system_memory_in_mb=`prtconf | awk '/Memory size:/ {print $3}'`
        system_cpu_cores=`psrinfo | wc -l`
    ;;
    Darwin)
        system_memory_in_bytes=`sysctl hw.memsize | awk '{print $2}'`
        system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
        system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
    ;;
    *)
        system_memory_in_mb="0"
        system_cpu_cores="0"
    ;;
esac

echo ${system_cpu_cores},${system_memory_in_mb}