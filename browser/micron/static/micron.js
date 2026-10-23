/*
 * Behaviour for Micron pages produced by micron/parser.py.
 *
 *   - growing text fields as the user types
 *   - collapsible headings
 *   - link clicks: collects form data exactly like NomadNet does, then
 *     fires a cancelable "micron:navigate" event so the host app can fetch
 *     the page over Reticulum. If nobody cancels it, in-page anchors scroll
 *     and nomadnet:// links navigate, with any request data as a query string.
 *
 * Call Micron.attach(pageElement) for pages inserted after load; pages in
 * the document at load time are attached automatically.
 */
(function () {
  "use strict";

  // Link fields spec ("name|other|var=value", "*" = all fields) ->
  // NomadNet request data: field_<name> for fields, var_<name> for variables.
  function collectRequestData(page, spec) {
    if (spec == null) return null;
    var entries = spec.split("|");
    var data = {};
    var names = [];
    var all = entries.indexOf("*") !== -1;
    entries.forEach(function (entry) {
      if (entry.indexOf("=") !== -1) {
        var parts = entry.split("=");
        if (parts.length === 2) data["var_" + parts[0]] = parts[1];
      } else {
        names.push(entry);
      }
    });

    page.querySelectorAll("[data-mu-field]").forEach(function (el) {
      var name = el.getAttribute("data-mu-field");
      if (!all && names.indexOf(name) === -1) return;
      var key = "field_" + name;
      if (el.type === "radio") {
        if (el.checked) data[key] = el.value;
      } else if (el.type === "checkbox") {
        if (el.checked) data[key] = data[key] ? data[key] + "," + el.value : el.value;
      } else {
        data[key] = el.value;
      }
    });
    return data;
  }

  function withQuery(href, data) {
    if (!data) return href;
    var keys = Object.keys(data);
    if (!keys.length) return href;
    var query = new URLSearchParams();
    keys.forEach(function (k) { query.append(k, data[k]); });
    return href + (href.indexOf("?") === -1 ? "?" : "&") + query.toString();
  }

  function scrollToAnchor(page, link, name) {
    var target;
    if (name) {
      target = page.querySelector("#mu-anchor-" + CSS.escape(name));
    } else {
      // A bare "#" jumps to the next heading after the link.
      var line = link.closest(".mu-line");
      for (var el = line && line.nextElementSibling; el; el = el.nextElementSibling) {
        if (el.classList.contains("mu-heading")) { target = el; break; }
      }
    }
    if (target) {
      unfoldAround(page, target.closest(".mu-line") || target);
      target.scrollIntoView({ block: "start" });
    }
  }

  function onLinkClick(page, event) {
    var link = event.target.closest("a.mu-link");
    if (!link || !page.contains(link)) return;
    event.preventDefault();

    var detail = {
      url: link.getAttribute("data-mu-url"),
      href: link.getAttribute("href"),
      requestData: collectRequestData(page, link.getAttribute("data-mu-fields")),
    };
    var navigate = new CustomEvent("micron:navigate", {
      bubbles: true, cancelable: true, detail: detail,
    });
    if (!link.dispatchEvent(navigate)) return;

    if (link.hasAttribute("data-mu-anchor")) {
      scrollToAnchor(page, link, link.getAttribute("data-mu-anchor"));
    } else if (detail.href) {
      window.location.href = withQuery(detail.href, detail.requestData);
    }
  }

  // Grow a field downwards to fit its text. It never shrinks on its own,
  // so a size the user dragged out with the handle is kept.
  function autoGrow(field) {
    if (field.tagName !== "TEXTAREA") return;
    if (field.scrollHeight > field.clientHeight) {
      // scrollHeight includes padding; height is content-box.
      var style = getComputedStyle(field);
      var padding = parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
      field.style.height = (field.scrollHeight - padding) + "px";
    }
  }

  // Show/hide lines under collapsed headings. A heading folds everything up
  // to the next heading of the same or a higher level (fewer ">").
  function applyFolds(page) {
    var hidingBelow = null;
    page.querySelectorAll(":scope > .mu-line").forEach(function (line) {
      var level = line.classList.contains("mu-heading")
        ? parseInt(line.getAttribute("data-level"), 10) : null;
      if (level !== null && hidingBelow !== null && level <= hidingBelow) hidingBelow = null;
      line.hidden = hidingBelow !== null;
      if (!line.hidden && level !== null && line.getAttribute("data-collapsed") === "true") {
        hidingBelow = level;
      }
    });
  }

  function unfoldAround(page, target) {
    if (!target.hidden) return;
    var lines = Array.prototype.slice.call(page.querySelectorAll(":scope > .mu-line"));
    var level = Infinity;
    for (var i = lines.indexOf(target) - 1; i >= 0; i--) {
      var line = lines[i];
      if (!line.classList.contains("mu-heading")) continue;
      var l = parseInt(line.getAttribute("data-level"), 10);
      if (l < level) {
        level = l;
        if (line.getAttribute("data-collapsed") === "true") line.setAttribute("data-collapsed", "false");
      }
    }
    applyFolds(page);
  }

  function onHeadingClick(page, event) {
    var heading = event.target.closest(".mu-collapsible");
    if (!heading || !page.contains(heading) || event.target.closest("a.mu-link")) return;
    var collapsed = heading.getAttribute("data-collapsed") === "true";
    heading.setAttribute("data-collapsed", collapsed ? "false" : "true");
    applyFolds(page);
  }

  function attach(page) {
    if (page.__micron) return;
    page.__micron = true;

    page.addEventListener("submit", function (e) { e.preventDefault(); });
    page.addEventListener("click", function (e) {
      onLinkClick(page, e);
      onHeadingClick(page, e);
    });
    page.addEventListener("input", function (e) {
      if (e.target.classList && e.target.classList.contains("mu-field")) autoGrow(e.target);
    });
    page.querySelectorAll("textarea.mu-field").forEach(autoGrow);
    applyFolds(page);
  }

  window.Micron = { attach: attach, collectRequestData: collectRequestData };

  function attachAll() { document.querySelectorAll(".mu-page").forEach(attach); }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", attachAll);
  } else {
    attachAll();
  }
})();
