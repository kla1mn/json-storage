import { Link, NavLink } from "react-router-dom";

export function Header() {
  const link = (to: string, label: string) => (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `px-3 py-2 rounded ${isActive ? "bg-gray-200" : "hover:bg-gray-100"}`
      }
    >
      {label}
    </NavLink>
  );
  return (
    <header className="border-b">
      <div className="mx-auto max-w-6xl px-4 h-14 flex items-center justify-between">
        <Link to="/" className="font-semibold">JSON Admin</Link>
        <nav className="flex gap-2">
          {link("/", "Home")}
          {link("/namespaces", "Namespaces")}
        </nav>
      </div>
    </header>
  );
}