import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
  start() {
    this.element.setAttribute("aria-busy", "true")
    this.element.classList.add("opacity-60", "pointer-events-none")
  }

  finish() {
    this.element.removeAttribute("aria-busy")
    this.element.classList.remove("opacity-60", "pointer-events-none")
  }
}
