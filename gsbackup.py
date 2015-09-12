#!/usr/bin/env python

import json
import pprint
import os
import shutil
import StringIO
import sys
import tempfile
import time

import boto
import gcs_oauth2_boto_plugin

CONFIG = dict()

class Object(object):
	def __init__(self, parent, name, size, full_path, uploaded=False):
		self.parent = parent
		self.name = name
		self.full_path = full_path
		self.uploaded = uploaded
		self.size = size

	def __eq__(self, other):
		return self.parent == other.parent and self.name == other.name

	def __hash__(self):
		return hash(self.name) + hash(self.parent)

	def to_json(self):
		return { 
			"type": "Object",
			"name": self.name,
			"uploaded": self.uploaded,
			"full_path": self.full_path,
			"size": self.size
		}

class Folder(object):
	def __init__(self, parent, name):
		self.parent = parent
		self.name = name
		self.children = []

	def __eq__(self, other):
		return self.parent == other.parent and self.name == other.name

	def __hash__(self):
		return hash(self.name) + hash(self.parent)

	def to_json(self):
		return { 
			"type": "Folder",
			"name": self.name,
			"children": [ c.to_json() for c in self.children ]
		}

class State(object):
	def __init__(self, root):
		self.root = root
		self.object_count = 0
		self.folder_count = 0

	def _add_folders(self, top, rest):
		if not rest:
			return top

		f = Folder(top, rest[0])

		if f not in top.children:
			top.children.append(f)
			self.folder_count += 1
		else:
			f = top.children[top.children.index(f)]

		return self._add_folders(f, rest[1:])

	def _add_directory(self, directory, filenames):
		split_path = directory.split("/")[1:]
		folder = self._add_folders(self.root, split_path)
		for fname in filenames:
			if fname == CONFIG['ignore_pattern']:
				continue
			full_path = os.path.join(CONFIG['source_strip_prefix'] + directory, fname)
			size = os.path.getsize(full_path)
			obj = Object(folder, fname, size, full_path)
			if obj not in folder.children: 
				self.object_count += 1
				folder.children.append(obj)

	def _collect_objects(self, start_from):
		objects = []
		def _collect(root, objects):
			for obj in root.children:
				if isinstance(obj, Object):
					objects.append(obj)
				elif isinstance(obj, Folder):
					_collect(obj, objects)
		_collect(start_from, objects)
		return objects

	def _find_folder(self, folder_name):
		def _find(root, path):
			if not path:
				return root
			for c in root.children:
				if isinstance(c, Folder) and c.name == path[0]:
					return _find(c, path[1:])

		return _find(self.root, folder_name.split('/'))

	def mark_as_uploaded(self, folder_name):
		folder = self._find_folder(folder_name)
		objects = self._collect_objects(folder)
		counter = 0
		for o in objects:
			o.uploaded = True
			counter += 1
		print '%d objects marked as uploaded' % counter

	def upload(self, uploader):
		for obj in [ o for o in self._collect_objects(self.root) if not o.uploaded ]:
			uploader.upload(obj)
			obj.uploaded = True
			self.persist(skip_backup=True)

	def print_not_uploaded(self):
		for obj in [ o for o in self._collect_objects(self.root) ]:
			if not obj.uploaded:
				print obj.full_path, obj.uploaded

	def stats(self):
		objects = self._collect_objects(self.root)
		print '%d objects' % len(objects)
		print '%d objects uploaded' % len([ o for o in objects if o.uploaded ])
		print '%d objects not uploaded' % len([ o for o in objects if not o.uploaded ])
		print '%s uploaded' % format_size(sum([o.size for o in objects if o.uploaded]))
		print '%s not uploaded' % format_size(sum([o.size for o in objects if not o.uploaded]))

	def refresh(self):
		for (dirpath, dirnames, filenames) in os.walk(CONFIG['source_directory']):
			self._add_directory(dirpath.replace(CONFIG['source_strip_prefix'], ''), filenames)

	def persist(self, skip_backup=False):
		db = CONFIG['state_file']
		if not skip_backup and os.path.exists(db):
			backup = '.' + db + '.' + str(int(time.time()))
			print 'Creating backup ' + backup
			os.rename(db, backup)
		open(db, 'w').write(json.dumps(state.root.to_json(), indent=True))

	@staticmethod
	def build():
		state = State(Folder("/", "/"))
		for (dirpath, dirnames, filenames) in os.walk(CONFIG['source_directory']):
			state._add_directory(dirpath.replace(CONFIG['source_strip_prefix'], ''), filenames)
		return state

	@staticmethod
	def from_file():
		def _add(obj, parent):
			if obj['type'] == 'Folder':
				folder = Folder(parent, obj['name'])
				state.folder_count += 1
				for c in obj['children']:
					_add(c, folder)
				parent.children.append(folder)
			elif obj['type'] == 'Object':
				state.object_count += 1
				name = obj['name']
				uploaded = obj['uploaded']
				size = None
				full_path = None
				if 'size' in obj: size = obj['size']
				if 'full_path' in obj: full_path = obj['full_path']
				parent.children.append(Object(parent, name, size, full_path, uploaded=uploaded))

		print "Loading", CONFIG['state_file']

		state = State(None)
		tree = json.load(open(CONFIG['state_file']))
		root = Folder("/", "/")

		for obj in tree['children']:
			_add(obj, root)

		state.root = root
		print "Loaded %d objects in %d folders" % (state.object_count, state.folder_count)
		return state

class GoogleStorageUploader(object):

	def __init__(self):
		gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret(CONFIG['client_id'], CONFIG['client_secret'])

	def upload(self, obj):
		sys.stdout.write('Uploading %s/%s %s... ' % (obj.parent.name, obj.name, format_size(obj.size)))
		sys.stdout.flush()
		with open(obj.full_path, 'r') as localfile:
			dest_uri = boto.storage_uri(CONFIG['bucket_id'] + obj.full_path, 'gs')
			dest_uri.new_key().set_contents_from_file(localfile)
		sys.stdout.write('Done!\n')

def format_size(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def usage():
	print 'gsbackup.py --config FILE COMMAND'
	print ''
	print 'Commands:'
	print '    --initial             Create initial database by scanning source directory'
	print '    --refresh             Refresh database by scanning source directory       '
	print '    --upload              Upload to Google Storage, abort with Ctrl-C         '
	print '    --list-not-uploaded   List files that have not been uploaded              '
	print ''

if __name__ == '__main__':

	state = None

	if len(sys.argv) > 3:
		if sys.argv[1] != '--config':
			usage()
			sys.exit(1)

		config_file = sys.argv[2]
		CONFIG = json.load(open(config_file))

		command = sys.argv[3]
		if command == '--initial':
			state = State.build()
			state.persist()
		elif command == '--refresh':
			state = State.from_file()
			state.refresh()
			state.persist()
		elif command == '--stats':
			state = State.from_file()
			state.stats()
		elif command == '--list-not-uploaded':
			state = State.from_file()
			state.print_not_uploaded()
		elif command == '--mark-as-uploaded':
			state = State.from_file()
			if len(sys.argv) != 5: 
				usage()
				sys.exit(1)
			folder = sys.argv[4]
			state.mark_as_uploaded(folder)
			state.persist()
		elif command == '--upload':
			try:
				state = State.from_file()
				state.upload(GoogleStorageUploader())
			except KeyboardInterrupt:
				state.persist()
		else:
			usage()
			sys.exit(1)
	else:
		usage()
		sys.exit(1)


