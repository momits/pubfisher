from typing import Optional

import gi
gi.require_version('Gdk', '3.0')
gi.require_version('Gtk', '3.0')
gi.require_version('Poppler', '0.18')
gi.require_version('EvinceDocument', '3.0')
gi.require_version('EvinceView', '3.0')
from gi.repository import Gdk, Gtk, Gio, Poppler, GObject, GLib
from gi.repository import EvinceDocument as ed
from gi.repository import EvinceView as ev

from pubfisher.gi_composites import GtkTemplate


class EPrintView(ev.View):
    def __init__(self):
        super().__init__()
        self.show()

    def view_file(self, file: Gio.File):
        flags = ed.DocumentLoadFlags.NONE
        doc = ed.Document.factory_get_document_for_gfile(file, flags)
        model = ev.DocumentModel.new_with_document(doc)
        self.set_model(model)


@GtkTemplate(ui='data/ui/edit_publications.ui')
class AnnotationWindow(Gtk.ApplicationWindow):
    __gtype_name__ = 'AnnotatePublicationsWindow'

    scrolled_window_eprint = GtkTemplate.Child()
    list_box_annotations = GtkTemplate.Child()
    revealer_view_options = GtkTemplate.Child()
    menu_zoom_level = GtkTemplate.Child()

    def __init__(self, file: Optional[Gio.File]=None, **kwargs):
        super().__init__(**kwargs)
        self.init_template()

        toggle_view_options = Gio.SimpleAction.new_stateful(
            'toggle-view-options', None, GLib.Variant.new_boolean(False))
        toggle_view_options.connect('activate', self._toggle_view_options)
        self.add_action(toggle_view_options)

        # screen = Gdk.Screen.get_default()
        # css_provider = Gtk.CssProvider()
        # css_provider.load_from_path('data/css/pubfisher.css')
        # context = Gtk.StyleContext()
        # context.add_provider_for_screen(screen,
        #                                 css_provider,
        #                                 Gtk.STYLE_PROVIDER_PRIORITY_USER)

        self.eprint_view = EPrintView()
        if file:
            self.eprint_view.view_file(file)
        self.scrolled_window_eprint.add(self.eprint_view)
        self.maximize()

    def _toggle_view_options(self, action, *args):
        is_currently_visible = action.get_state()
        self.revealer_view_options.set_reveal_child(not is_currently_visible)
        action.set_state(GLib.Variant.new_boolean(not is_currently_visible))

    @GtkTemplate.Callback
    def _on_zoom_entry_menu_requested(self, entry, event, *args):
        popover = Gtk.Popover.new_from_model(entry, self.menu_zoom_level)
        rect = entry.get_icon_area(Gtk.EntryIconPosition.SECONDARY)
        

    @GtkTemplate.Callback
    def _on_zoom_entry_activated(self, entry, *args):
        pass

    @GtkTemplate.Callback
    def _on_zoom_entry_focused_out(self, entry, *args):
        pass


class Annotate(Gtk.Application):
    def __init__(self):
        Gtk.Application.__init__(self,
                                 application_id='de.momits.pubfisher.Annotate',
                                 flags=Gio.ApplicationFlags.HANDLES_OPEN)
        ed.init()

    def do_open(self, files: [Gio.File], *args):
        for file in files:
            win = AnnotationWindow(file=file, application=self)
            win.present()

    def do_activate(self):
        win = self.props.active_window
        if not win:
            win = AnnotationWindow(application=self)
        win.present()
