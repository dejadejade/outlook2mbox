// +build windows

package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	ole "github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
)

var (
	CLSID_IConverterSession = ole.NewGUID("{4e3a7680-b77a-11d0-9da5-00c04fd65685}")
	IID_IConverterSession   = ole.NewGUID("{4b401570-b77b-11d0-9da5-00c04fd65685}")
	IID_IMessage            = ole.NewGUID("{00020307-0000-0000-C000-000000000046}")
	IID_IStream             = ole.NewGUID("{0000000C-0000-0000-C000-000000000046}")

	modole32, _               = syscall.LoadDLL("ole32.dll")
	pCreateStreamOnHGlobal, _ = modole32.FindProc("CreateStreamOnHGlobal")
	pGetHGlobalFromStream, _  = modole32.FindProc("GetHGlobalFromStream")
	pCoCreateInstance, _      = modole32.FindProc("CoCreateInstance")

	modmapi32, _       = syscall.LoadDLL("mapi32.dll")
	pMAPIInitialize, _ = modmapi32.FindProc("MAPIInitialize")
	pMAPILogonEx, _    = modmapi32.FindProc("MAPILogonEx")

	modkernel32, _   = syscall.LoadDLL("kernel32.dll")
	pGlobalLock, _   = modkernel32.FindProc("GlobalLock")
	pGlobalUnlock, _ = modkernel32.FindProc("GlobalUnlock")

	listFolders    = flag.Bool("list", false, "list folders")
	useAddressBook = flag.Bool("ab", false, "use addressbook to translate email address")

	saveFolder      = flag.String("folder", "", "folder name to save")
	targetDirectory = flag.String("dir", ".", "target directory to save")
	count           = flag.Int("count", 1000, "total emails to save")
	startDate       = flag.String("startdate", "", "start date of emails to save (e.g., 20060102)")
	endDate         = flag.String("enddate", "", "end date of emails to save (e.g., 20060102)")
)

type Folder struct {
	EntryID                          string
	Name, Path                       string
	NumFolders, NumItems, TotalItems int
	Parent                           *Folder `json:"-"`
	Children                         []*Folder
	Store, StorePath                 string
	DefaultItemType                  int
	DefaultMessageClass              string
	Class                            int
	IDispatch                        *ole.IDispatch `json:"-"`
}

var POSTMARK = []byte{'\x01', '\x01', '\x01', '\x01', '\n'} // "\x01\x01\x01\x01\n"

func main() {
	flag.Parse()
	ole.CoInitialize(0)
	pMAPIInitialize.Call(uintptr(0))
	runtime.LockOSThread()

	converter, err := createConverterSession()
	if err != nil {
		log.Printf("createConverterSession: %v\n", err)
		return
	}

	sess, err := createMAPISession()
	if err != nil {
		log.Printf("createMAPISession: %v\n", err)
		return
	}

	app, err := oleutil.CreateObject("Outlook.Application")
	if err != nil {
		log.Printf("Failed to create Outlook application: %v\n", err)
		return
	}

	disp, _ := app.QueryInterface(ole.IID_IDispatch)
	log.Printf("Name: %v, Version: %v, ProductCode: %v, DefaultProfileName: %v\n",
		oleutil.MustGetProperty(disp, "Name").Value(),
		oleutil.MustGetProperty(disp, "Version").Value(),
		oleutil.MustGetProperty(disp, "ProductCode").Value(),
		oleutil.MustGetProperty(disp, "DefaultProfileName").Value())

	ns := oleutil.MustCallMethod(disp, "GetNamespace", "MAPI").ToIDispatch()

	folders, _ := getFolders(ns, nil)
	if *listFolders {
		for i, f := range folders {
			log.Printf("%d: %s %s (%d)\n", i, f.Name, f.Path, f.TotalItems)
		}
	}

	if *saveFolder == "" {
		return
	}

	var folder *Folder
	for _, f := range folders {
		if f.Name == *saveFolder {
			folder = f
			break
		}
	}

	if folder == nil {
		log.Printf("Folder %s not found\n", *saveFolder)
		return
	}

	if *useAddressBook {
		ab, err := sess.OpenAddressBook()
		if err != nil {
			log.Printf("OpenAddressBook:%v: %v\n", err, ab)
		}
		if ab != nil {
			converter.SetAdrBook(ab)
		}
	}

	if err = os.MkdirAll(*targetDirectory, 0755); err != nil {
		log.Printf("Failed to make dir for %s: %v\n", *targetDirectory, err)
		return
	}

	var stm *IStream
	hr, _, _ := pCreateStreamOnHGlobal.Call(uintptr(0), uintptr(0), uintptr(unsafe.Pointer(&stm)))
	if hr != 0 {
		log.Printf("CreateStreamOnHGlobal: %v\n", ole.NewError(hr))
		return
	}

	items := oleutil.MustCallMethod(folder.IDispatch, "Items").ToIDispatch()
	defer items.Release()

	oleutil.CallMethod(items, "Sort", "CreationTime", false)
	total := oleutil.MustGetProperty(items, "Count").Value().(int32)

	xstart := 0
	if *startDate != "" {
		if d, err := time.Parse("20060102", *startDate); err == nil {
			if pos, err := findFirstItemAfter(items, int(total), d.Unix()); err == nil {
				log.Printf("Starting from %d for %s\n", pos, *startDate)
				xstart = pos
			}
		}
	}

	xend := xstart + *count
	if *endDate != "" {
		if d, err := time.Parse("20060102", *endDate); err == nil {
			if pos, err := findFirstItemAfter(items, int(total), d.Unix()); err == nil {
				log.Printf("Stopping by %d for %s\n", pos, *endDate)
				xend = pos
				*count = xend - xstart
			}
		}
	}

	log.Printf("Folder %s: total %d items, from: %d, to: %d, count: %d\n", folder.Name, total, xstart, xend, *count)

	saved := 0

	var fout *File
	for i := xstart; i < xend; {
		if i >= int(total) {
			break
		}

		idx := i + 1
		item, err := oleutil.GetProperty(items, "Item", idx)
		if err != nil || item.VT != ole.VT_DISPATCH {
			log.Printf("Failed to get Item %d: %v\n", idx, err)
			continue
		}

		obj := item.ToIDispatch()
		data, ts, err, stop := extractMessageData(obj, converter, stm)

		if err != nil {
			var subject, mclass string
			if value, err := oleutil.GetProperty(obj, "Subject"); err == nil {
				subject, _ = value.Value().(string)
			}
			if value, err := oleutil.GetProperty(obj, "MessageClass"); err == nil {
				mclass, _ = value.Value().(string)
			}

			log.Printf("Failed to extract data for %d %s (%s): %v\n", i, subject, mclass, err)
		}

		obj.Release()

		if stop {
			log.Printf("Stopped at %d\n", idx)
			break
		}

		i += 1
		if err != nil || len(data) == 0 {
			continue
		}

		if fout != nil && !ts.IsZero() && !equalMonth(ts, fout.timestamp) {
			fout.Close()
			debug.FreeOSMemory()
			fout = nil
		}

		if fout == nil {
			if fout, err = newFile(*targetDirectory, folder.Name, ts); err != nil {
				break
			}
		}
		fout.zf.Write(POSTMARK)
		fout.zf.Write(data)
		fout.zf.Write(POSTMARK)
		saved += 1
	}

	if fout != nil {
		fout.Close()
	}

	log.Printf("%d emails saved\n", saved)
}

type File struct {
	zf        io.WriteCloser
	f         *os.File
	path      string
	timestamp time.Time
}

func equalMonth(d1, d2 time.Time) bool {
	y1, m1, _ := d1.Date()
	y2, m2, _ := d2.Date()
	return y1 == y2 && m1 == m2

}

func (f *File) Close() {
	if f.zf != nil {
		f.zf.Close()
		f.zf = nil
	}
	if f.f != nil {
		f.f.Close()
		f.f = nil
	}
	f.path = ""
}

func newFile(dir, name string, ts time.Time) (f *File, err error) {
	fname := fmt.Sprintf("%s_%s.mmdf.gz", name, ts.Format("200601"))
	fpath := filepath.Join(dir, fname)
	fout, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Failed to open file %s: %v\n", fpath, err)
		return nil, err
	}

	zout := gzip.NewWriter(fout)
	log.Printf("Opening file %s\n", fpath)
	return &File{zf: zout, f: fout, path: fpath, timestamp: ts}, nil
}

// http://msdn2.microsoft.com/en-us/library/bb905202.aspx
type IConverterSession struct {
	ole.IUnknown
}

type IConverterSessionVtbl struct {
	ole.IUnknownVtbl
	SetAdrBook      uintptr
	SetEncoding     uintptr
	PlaceHolder1    uintptr
	MIMEToMAPI      uintptr
	MAPIToMIMEStm   uintptr
	PlaceHolder2    uintptr
	PlaceHolder3    uintptr
	PlaceHolder4    uintptr
	SetTextWrapping uintptr
	SetSaveFormat   uintptr
	PlaceHolder5    uintptr
	SetCharset      uintptr
}

func (v *IConverterSession) VTable() *IConverterSessionVtbl {
	return (*IConverterSessionVtbl)(unsafe.Pointer(v.RawVTable))
}

func (v *IConverterSession) SetEncoding(et uint32) (err error) {
	hr, _, _ := syscall.Syscall(
		v.VTable().SetEncoding,
		2,
		uintptr(unsafe.Pointer(v)),
		uintptr(et), 0)
	if hr != 0 {
		err = ole.NewError(hr)
		fmt.Println(err)
	}
	return
}

func (v *IConverterSession) SetSaveFormat(et uint32) (err error) {
	hr, _, _ := syscall.Syscall(
		v.VTable().SetSaveFormat,
		2,
		uintptr(unsafe.Pointer(v)),
		uintptr(et), 0)
	if hr != 0 {
		err = ole.NewError(hr)
		fmt.Println(err)
	}
	return
}

func (v *IConverterSession) SetTextWrapping(wrap bool, width uint32) (err error) {
	var wrapIt uint32 = 0
	if wrap {
		wrapIt = 1
	}
	hr, _, _ := syscall.Syscall(
		v.VTable().SetTextWrapping,
		3,
		uintptr(unsafe.Pointer(v)),
		uintptr(wrapIt), uintptr(width))
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

func (v *IConverterSession) MAPIToMIMEStm(msg *ole.IDispatch, stm *IStream, flags uint32) (err error) {
	hr, _, _ := syscall.Syscall6(
		v.VTable().MAPIToMIMEStm,
		4,
		uintptr(unsafe.Pointer(v)),
		uintptr(unsafe.Pointer(msg)),
		uintptr(unsafe.Pointer(stm)),
		uintptr(flags), 0, 0)
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

func (v *IConverterSession) SetAdrBook(ab *ole.IUnknown) (err error) {
	hr, _, _ := syscall.Syscall(
		v.VTable().SetAdrBook,
		2,
		uintptr(unsafe.Pointer(v)),
		uintptr(unsafe.Pointer(ab)),
		0)
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

type IStream struct {
	ole.IUnknown
}

type IStreamVtbl struct {
	ole.IUnknownVtbl
	Read         uintptr
	Write        uintptr
	Seek         uintptr
	SetSize      uintptr
	CopyTo       uintptr
	Commit       uintptr
	Revert       uintptr
	LockRegion   uintptr
	UnlockRegion uintptr
	Stat         uintptr
	Clone        uintptr
}

func (v *IStream) VTable() *IStreamVtbl {
	return (*IStreamVtbl)(unsafe.Pointer(v.RawVTable))
}

func (v *IStream) Seek(move int32, origin uint32) (pos uint64, err error) {
	hr, _, _ := syscall.Syscall6(
		v.VTable().Seek,
		4,
		uintptr(unsafe.Pointer(v)),

		0,
		uintptr(origin),
		uintptr(unsafe.Pointer(&pos)), 0, 0)
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

func (v *IStream) Write(buf []byte) (written uint64, err error) {
	hr, _, _ := syscall.Syscall6(
		v.VTable().Write,
		4,
		uintptr(unsafe.Pointer(v)),
		uintptr(unsafe.Pointer(&buf)),
		uintptr(len(buf)),
		uintptr(unsafe.Pointer(&written)), 0, 0)
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

type IMAPISession struct {
	ole.IUnknown
}

type IMAPISessionVtbl struct {
	ole.IUnknownVtbl
	GetLastError           uintptr
	GetMsgStoresTable      uintptr
	OpenMsgStore           uintptr
	OpenAddressBook        uintptr
	OpenProfileSection     uintptr
	GetStatusTable         uintptr
	OpenEntry              uintptr
	CompareEntryIDs        uintptr
	Advise                 uintptr
	Unadvise               uintptr
	MessageOptions         uintptr
	QueryDefaultMessageOpt uintptr
	EnumAdrTypes           uintptr
	QueryIdentity          uintptr
	Logoff                 uintptr
	SetDefaultStore        uintptr
	AdminServices          uintptr
	ShowForm               uintptr
	PrepareForm            uintptr
}

func (v *IMAPISession) VTable() *IMAPISessionVtbl {
	return (*IMAPISessionVtbl)(unsafe.Pointer(v.RawVTable))
}

func (v *IMAPISession) OpenAddressBook() (ab *ole.IUnknown, err error) {
	hr, _, _ := syscall.Syscall6(
		v.VTable().OpenAddressBook,
		5,
		uintptr(unsafe.Pointer(v)),
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&ab)), 0)

	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

func createConverterSession() (converter *IConverterSession, err error) {
	hr, _, _ := pCoCreateInstance.Call(uintptr(unsafe.Pointer(CLSID_IConverterSession)), 0, ole.CLSCTX_INPROC_SERVER,
		uintptr(unsafe.Pointer(IID_IConverterSession)), uintptr(unsafe.Pointer(&converter)))
	if hr != 0 {
		err = ole.NewError(hr)
		return
	}

	const (
		SAVE_RFC1521 = 1
		IET_QP       = 3
	)

	converter.SetSaveFormat(SAVE_RFC1521)
	converter.SetEncoding(IET_QP)
	converter.SetTextWrapping(true, 74)

	return
}

func createMAPISession() (sess *IMAPISession, err error) {
	var flags uint32 = 0x00000020 //| 0x00000002 // |0x00000001 // MAPI_EXTENDED | MAPI_NO_MAIL
	hr, _, _ := pMAPILogonEx.Call(uintptr(0), 0, 0, uintptr(flags), uintptr(unsafe.Pointer(&sess)))
	if hr != 0 {
		err = ole.NewError(hr)
	}
	return
}

func getFolders(root *ole.IDispatch, parent *Folder) (ofolders []*Folder, ofolder *Folder) {
	var name, path string

	if value, err := oleutil.GetProperty(root, "Name"); err == nil {
		name = value.Value().(string)
	}
	if value, err := oleutil.GetProperty(root, "FolderPath"); err == nil {
		path = value.Value().(string)
	}

	ofolder = &Folder{Name: name, Path: path, IDispatch: root, Parent: parent}
	ofolders = append(ofolders, ofolder)

	if value, err := oleutil.GetProperty(root, "Class"); err == nil && value.VT == ole.VT_I4 && ofolder != nil {
		ofolder.Class = int(value.Value().(int32))
	}

	if value, err := oleutil.GetProperty(root, "DefaultItemType"); err == nil {
		ofolder.DefaultItemType = int(value.Value().(int32))
	}

	if value, err := oleutil.GetProperty(root, "DefaultMessageClass"); err == nil {
		ofolder.DefaultMessageClass = value.Value().(string)
	}

	if value, err := oleutil.GetProperty(root, "Store"); err == nil && value.VT == ole.VT_DISPATCH && ofolder != nil {
		store := value.ToIDispatch()
		if name, err := oleutil.GetProperty(store, "DisplayName"); err == nil {
			ofolder.Store = name.Value().(string)
		}
		if name, err := oleutil.GetProperty(store, "FilePath"); err == nil {
			ofolder.StorePath = name.Value().(string)
		}
		store.Release()
	}

	if folders, err := oleutil.GetProperty(root, "Folders"); err == nil {
		xfolders := folders.ToIDispatch()
		nfolders := oleutil.MustGetProperty(xfolders, "Count").Value().(int32)
		if ofolder != nil {
			ofolder.NumFolders = int(nfolders)
		}
		for i := 1; i <= int(nfolders); i++ {
			item, err := oleutil.GetProperty(xfolders, "Item", i)
			if err != nil || item.VT != ole.VT_DISPATCH {
				log.Println(err)
				continue
			}

			xitem := item.ToIDispatch()
			xs, x := getFolders(xitem, ofolder)
			ofolders = append(ofolders, xs...)
			ofolder.Children = append(ofolder.Children, x)
			ofolder.TotalItems += x.TotalItems
		}
		xfolders.Release()
	}

	if items, err := oleutil.GetProperty(root, "Items"); err == nil {
		xitems := items.ToIDispatch()
		nitems := oleutil.MustGetProperty(xitems, "Count").Value().(int32)
		ofolder.NumItems = int(nitems)
		ofolder.TotalItems += ofolder.NumItems
		xitems.Release()
	}

	return
}

func findFirstItemAfter(items *ole.IDispatch, count int, timestamp int64) (idx int, err error) {
	datef := func(i int) (ts int64) {
		item, err := oleutil.GetProperty(items, "Item", i+1)
		if err != nil || item.VT != ole.VT_DISPATCH {
			log.Printf("GetPoperty: %v", err)
			return
		}

		obj := item.ToIDispatch()
		defer obj.Release()

		if value, err := oleutil.GetProperty(obj, "CreationTime"); err == nil && value.VT == ole.VT_DATE {
			if t, ok := value.Value().(time.Time); ok {
				log.Println(t)
				ts = t.Unix()
				return
			}
		}
		return
	}

	idx = sort.Search(count, func(i int) bool { return datef(i) > timestamp })
	if idx >= count {
		idx = count
	} else {
		log.Printf("Found item: %d\n", idx)
	}
	return
}

func extractMessageData(obj *ole.IDispatch, converter *IConverterSession, stm *IStream) (data []byte, ts time.Time, err error, stop bool) {
	const CCSF_SMTP = 2

	stm.Seek(0, 0)

	value, err := oleutil.GetProperty(obj, "MessageClass")
	if err == nil && strings.HasPrefix(value.Value().(string), "IPM.Schedule.Meeting.Resp.") {
		return
	}

	if value, err := oleutil.GetProperty(obj, "CreationTime"); err == nil && value.VT == ole.VT_DATE {
		if t, ok := value.Value().(time.Time); ok {
			ts = t
		}
	}

	value, err = oleutil.GetProperty(obj, "MAPIOBJECT")
	if err != nil {
		log.Printf("Get MAPIOBJECT: %v\n", err)
		stop = true
		return
	}

	mobj := value.Value().(*ole.IUnknown)
	defer mobj.Release()

	var imsg *ole.IDispatch
	imsg, err = mobj.QueryInterface(IID_IMessage)
	if err != nil {
		log.Printf("QueryInterface: %v\n", err)
		return
	}
	defer imsg.Release()

	err = converter.MAPIToMIMEStm(imsg, stm, CCSF_SMTP)
	if err != nil {
		log.Printf("MAPIToMIMEStm: %v\n", err)
		return
	}

	var size uint64
	size, err = stm.Seek(0, 1)
	if err != nil || size <= 0 {
		log.Printf("Seek: %v\n", err)
		return
	}

	var handle uintptr
	hr, _, _ := pGetHGlobalFromStream.Call(uintptr(unsafe.Pointer(stm)), uintptr(unsafe.Pointer(&handle)))
	if hr != 0 {
		log.Printf("GetHGlobalFromStream: %v\n", ole.NewError(hr))
		return
	}

	addr, _, _ := pGlobalLock.Call(handle)
	if addr == 0 {
		log.Println("Unable to GlobalLock")
		return
	}
	defer pGlobalUnlock.Call(handle)

	buf := (*[1 << 30]byte)(unsafe.Pointer(uintptr(addr)))[0:size]
	data = make([]byte, size)
	copy(data, buf)
	return
}
