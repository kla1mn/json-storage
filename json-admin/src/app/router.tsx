import { createBrowserRouter } from "react-router-dom";
import { HomePage } from "../pages/HomePage";
import { NamespacesPage } from "../pages/NamespacePage";

export const router = createBrowserRouter([
  { path: "/", element: <HomePage /> },
  { path: "/namespaces", element: <NamespacesPage /> },
]);