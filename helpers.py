def find_matching_lines(line, keyword):

   if line.startswith(keyword):

      yield line

def split_package_name(package_name):

   result = []

   end = package_name.find('.')

   while end > 0:

      result.append(package_name[0:end])

      end = package_name.find('.', end + 1)

   result.append(package_name)

   return result

def resolve_packages(line, keyword):

   start = line.find(keyword) + len(keyword)

   end = line.find(';', start)

   if start < end:

      package_name = line[start:end].strip()

      return split_package_name(package_name)

   return []

def resolve_package_usage(line, keyword):

   packages = resolve_packages(line, keyword)

   for p in packages:

      yield (p, 1)

def compare_by_value(kv1, kv2):

   key1, value1 = kv1

   key2, value2 = kv2

   return value1 < value2
